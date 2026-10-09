//! Native output rights driven through real IO transitions and completions.

use alloc::boxed::Box;

use skein_lib::stream::{Down, Fault, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Time, Token};

use super::{Kind, Out, Rig, connected, filled, limits, owner};
use crate::kernel::{Done, Error, Fd, Op};
use crate::{Event, Limits, Request};

const FIRST: Token = Token::new(10);
const SECOND: Token = Token::new(11);
const WRONG: Token = Token::new(99);

fn output(rig: &mut Rig, stream: Token, down: OutputDown) -> Out {
    rig.down(Request::Output { stream, down })
}

fn settled(right: Token, outcome: OutputOutcome) -> Event {
    Event::Output { owner: owner(1), up: OutputUp::Settled { right, outcome } }
}

fn grant(rig: &mut Rig, stream: Token, right: Token, bytes: u32) {
    output(rig, stream, OutputDown::Room { right, bytes }).nothing();
    assert_eq!(rig.next().events, [settled(right, OutputOutcome::Granted)]);
}

#[test]
fn output_moves_while_a_classic_read_is_unanswered_then_the_exact_read_arrives() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), Fd::new(3));
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read: Read::Fill(2), room: 0 } }).nothing();
    rig.next().nothing();
    grant(&mut rig, socket, FIRST, 8);
    let send = output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"response"[..]) })
        .take(Kind::Send);
    assert_eq!(super::buffer(&send.kind), (&b"response"[..], 0));
    rig.complete(send, Ok(Done::Count(8))).nothing();
    assert_eq!(
        filled(&mut rig, recv, b"ok").events,
        [Event::Stream { owner: owner(1), up: Up::Bytes(Box::from(&b"ok"[..])) }],
        "output did not answer or withdraw the read"
    );
}

#[test]
fn a_read_answer_while_output_room_waits_does_not_settle_that_output() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), Fd::new(3));
    grant(&mut rig, socket, FIRST, 8);
    let send = output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"response"[..]) })
        .take(Kind::Send);
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read: Read::Fill(2), room: 0 } }).nothing();
    output(&mut rig, socket, OutputDown::Room { right: SECOND, bytes: 1 }).nothing();
    rig.next().nothing();
    output(&mut rig, socket, OutputDown::Send { right: SECOND, bytes: Box::from(&b"oversized before grant"[..]) })
        .nothing();
    assert_eq!(
        filled(&mut rig, recv, b"ok").events,
        [Event::Stream { owner: owner(1), up: Up::Bytes(Box::from(&b"ok"[..])) }]
    );
    assert_eq!(rig.complete(send, Ok(Done::Count(8))).events, [settled(SECOND, OutputOutcome::Granted)]);
    output(&mut rig, socket, OutputDown::Release { right: SECOND }).nothing();
}

#[test]
fn bytes_end_and_an_independent_terminal_reach_the_declared_three_event_maximum() {
    let mut rig = Rig::new(limits());
    let (socket, recv) = connected(&mut rig, owner(1), Fd::new(3));
    let recv = filled(&mut rig, recv, b"ok").take(Kind::Recv);
    filled(&mut rig, recv, b"").nothing();
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read: Read::Fill(2), room: 0 } }).nothing();
    output(&mut rig, socket, OutputDown::Room { right: FIRST, bytes: 8 }).nothing();
    assert_eq!(
        rig.next().events,
        [
            Event::Stream { owner: owner(1), up: Up::Bytes(Box::from(&b"ok"[..])) },
            Event::Stream { owner: owner(1), up: Up::End },
            settled(FIRST, OutputOutcome::Granted),
        ]
    );
    output(&mut rig, socket, OutputDown::Release { right: FIRST }).nothing();
    grant(&mut rig, socket, SECOND, 8);
}

#[test]
fn in_flight_bytes_block_room_with_one_short_and_free_queued_slot_positive_controls() {
    for (requested, slots, immediate) in [(7, 1, true), (8, 1, false)] {
        let mut rig = Rig::new(Limits { sends: slots, ..limits() });
        let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
        grant(&mut rig, socket, FIRST, 1);
        let send =
            output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"x"[..]) }).take(Kind::Send);
        output(&mut rig, socket, OutputDown::Room { right: SECOND, bytes: requested }).nothing();
        let out = rig.next();
        if immediate {
            assert_eq!(out.events, [settled(SECOND, OutputOutcome::Granted)]);
            output(&mut rig, socket, OutputDown::Release { right: SECOND }).nothing();
            rig.complete(send, Ok(Done::Count(1))).nothing();
        } else {
            out.nothing();
            assert_eq!(rig.complete(send, Ok(Done::Count(1))).events, [settled(SECOND, OutputOutcome::Granted)]);
        }
    }
}

#[test]
fn cancellation_before_grant_wins_once_and_cannot_release_the_next_grant() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    output(&mut rig, socket, OutputDown::Room { right: FIRST, bytes: 8 }).nothing();
    output(&mut rig, socket, OutputDown::Cancel { right: WRONG }).nothing();
    output(&mut rig, socket, OutputDown::Cancel { right: FIRST }).nothing();
    output(&mut rig, socket, OutputDown::Cancel { right: FIRST }).nothing();
    assert_eq!(rig.next().events, [settled(FIRST, OutputOutcome::Cancelled)]);
    grant(&mut rig, socket, SECOND, 8);
    output(&mut rig, socket, OutputDown::Cancel { right: FIRST }).nothing();
    output(&mut rig, socket, OutputDown::Release { right: FIRST }).nothing();
    output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"oversized stale"[..]) }).nothing();
    rig.next().nothing();
    let send = output(&mut rig, socket, OutputDown::Send { right: SECOND, bytes: Box::from(&b"response"[..]) })
        .take(Kind::Send);
    rig.complete(send, Ok(Done::Count(8))).nothing();
}

#[test]
fn a_grant_already_queued_above_wins_over_cancel_and_accepts_only_one_send() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    output(&mut rig, socket, OutputDown::Room { right: FIRST, bytes: 8 }).nothing();
    let queued = rig.next();
    output(&mut rig, socket, OutputDown::Cancel { right: FIRST }).nothing();
    rig.next().nothing();
    assert_eq!(queued.events, [settled(FIRST, OutputOutcome::Granted)]);
    output(&mut rig, socket, OutputDown::Send { right: WRONG, bytes: Box::from(&b"oversized stale"[..]) }).nothing();
    let send =
        output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"x"[..]) }).take(Kind::Send);
    output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"oversized stale"[..]) }).nothing();
    rig.next().nothing();
    rig.complete(send, Ok(Done::Count(1))).nothing();
}

#[test]
fn an_empty_send_and_release_each_retire_the_grant_and_classic_empty_send_releases_ownership() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    rig.down(Request::Stream { stream: socket, down: Down::Demand { read: Read::Nothing, room: 8 } }).nothing();
    assert_eq!(rig.next().events, [Event::Stream { owner: owner(1), up: Up::Room }]);
    output(&mut rig, socket, OutputDown::Room { right: WRONG, bytes: 8 }).nothing();
    rig.next().nothing();
    rig.down(Request::Stream { stream: socket, down: Down::Send(Box::default()) }).nothing();
    grant(&mut rig, socket, FIRST, 8);
    output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::default() }).nothing();
    grant(&mut rig, socket, SECOND, 8);
    output(&mut rig, socket, OutputDown::Release { right: SECOND }).nothing();
    output(&mut rig, socket, OutputDown::Room { right: WRONG, bytes: 0 }).nothing();
    output(&mut rig, socket, OutputDown::Room { right: WRONG, bytes: 9 }).nothing();
    output(&mut rig, socket, OutputDown::Send { right: SECOND, bytes: Box::from(&b"oversized stale"[..]) }).nothing();
    rig.next().nothing();
}

#[test]
fn genuine_write_failure_settles_waiting_output_before_classic_failed() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    grant(&mut rig, socket, FIRST, 8);
    let send = output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"response"[..]) })
        .take(Kind::Send);
    output(&mut rig, socket, OutputDown::Room { right: SECOND, bytes: 1 }).nothing();
    rig.next().nothing();
    assert_eq!(
        rig.complete(send, Err(Error::Reset)).events,
        [
            settled(SECOND, OutputOutcome::Failed(Fault::Reset)),
            Event::Stream { owner: owner(1), up: Up::Failed(Fault::Reset) }
        ]
    );
    output(&mut rig, socket, OutputDown::Send { right: SECOND, bytes: Box::from(&b"oversized stale"[..]) }).nothing();
    output(&mut rig, socket, OutputDown::Cancel { right: SECOND }).nothing();
    rig.next().nothing();
}

#[test]
fn full_actual_slot_envelope_waits_and_one_free_queued_slot_is_a_positive_control() {
    for (slots, immediate) in [(1, false), (2, true)] {
        let mut rig = Rig::new(Limits { sends: slots, ..limits() });
        let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
        grant(&mut rig, socket, FIRST, 1);
        let send =
            output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"x"[..]) }).take(Kind::Send);
        grant(&mut rig, socket, SECOND, 1);
        output(&mut rig, socket, OutputDown::Send { right: SECOND, bytes: Box::from(&b"y"[..]) }).nothing();
        output(&mut rig, socket, OutputDown::Room { right: WRONG, bytes: 1 }).nothing();
        let ready = rig.next();
        if immediate {
            assert_eq!(ready.events, [settled(WRONG, OutputOutcome::Granted)]);
            output(&mut rig, socket, OutputDown::Release { right: WRONG }).nothing();
        } else {
            ready.nothing();
        }
        let mut completed = rig.complete(send, Ok(Done::Count(1)));
        if immediate {
            assert!(completed.events.is_empty());
        } else {
            assert_eq!(completed.events, [settled(WRONG, OutputOutcome::Granted)]);
        }
        let send = completed.take(Kind::Send);
        rig.complete(send, Ok(Done::Count(1))).nothing();
    }
}

#[test]
fn stalled_and_partial_writes_preserve_actual_bytes_until_the_exact_buffer_finishes() {
    let mut rig = Rig::new(Limits { sends: 1, ..limits() });
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    grant(&mut rig, socket, FIRST, 8);
    let send = output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"response"[..]) })
        .take(Kind::Send);
    rig.complete(send, Err(Error::NoBufferSpace)).nothing();
    output(&mut rig, socket, OutputDown::Room { right: SECOND, bytes: 1 }).nothing();
    rig.next().nothing();
    let send = rig.at(Time::ZERO.saturating_add(limits().retry)).take(Kind::Send);
    let continued = rig.complete(send, Ok(Done::Count(3))).take(Kind::Send);
    assert_eq!(super::buffer(&continued.kind), (&b"response"[..], 3));
    assert_eq!(rig.complete(continued, Ok(Done::Count(5))).events, [settled(SECOND, OutputOutcome::Granted)]);
}

#[test]
fn close_and_abort_carry_the_pending_terminal_before_actual_closed_without_a_second_answer() {
    for abort in [false, true] {
        let mut rig = Rig::new(limits());
        let (socket, recv) = connected(&mut rig, owner(1), Fd::new(3));
        assert_eq!(filled(&mut rig, recv, b"").events, [Event::Stream { owner: owner(1), up: Up::End }]);
        output(&mut rig, socket, OutputDown::Room { right: FIRST, bytes: 8 }).nothing();
        let request = if abort { Request::Abort { entity: socket } } else { Request::Close { entity: socket } };
        let mut closing = rig.down(request);
        assert_eq!(rig.next().events, [settled(FIRST, OutputOutcome::Cancelled)]);
        let close = if abort {
            closing.take(Kind::Close)
        } else {
            let shutdown = closing.take(Kind::Shutdown);
            rig.complete(shutdown, Ok(Done::Nothing)).take(Kind::Close)
        };
        assert_eq!(rig.complete(close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
        output(&mut rig, socket, OutputDown::Room { right: SECOND, bytes: 8 }).nothing();
        output(&mut rig, socket, OutputDown::Cancel { right: FIRST }).nothing();
        output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"oversized stale"[..]) })
            .nothing();
        rig.next().nothing();
        rig.empty();
    }
}

#[test]
#[should_panic(expected = "a matching independent Send fits its grant")]
fn only_a_matching_oversized_send_is_an_invariant_violation() {
    let mut rig = Rig::new(limits());
    let (socket, _recv) = connected(&mut rig, owner(1), Fd::new(3));
    grant(&mut rig, socket, FIRST, 1);
    output(&mut rig, socket, OutputDown::Send { right: FIRST, bytes: Box::from(&b"xx"[..]) }).nothing();
}

fn child_command() -> crate::kernel::Spawn {
    let mut pipes = skein_lib::List::with_capacity(2);
    pipes.push(crate::kernel::Pipe { child: 0, way: crate::kernel::Way::In, parent: None }).expect("input pipe slot");
    pipes.push(crate::kernel::Pipe { child: 1, way: crate::kernel::Way::Out, parent: None }).expect("output pipe slot");
    crate::kernel::Spawn {
        program: Box::from(&b"/bin/cat"[..]),
        args: Box::default(),
        env: Box::default(),
        root: Fd::new(3),
        dir: Box::from(&b"."[..]),
        pipes: pipes.into_boxed(),
    }
}

#[test]
fn native_pipe_close_settles_waiting_output_and_actual_reap_waits_for_both_closed_pipes() {
    let mut rig = Rig::new(Limits { sockets: 3, ..limits() });
    let mut spawn = rig.down(Request::Spawn { owner: owner(1), spawn: child_command() }).take(Kind::Spawn);
    super::process::parent_ends(&mut spawn, &[Fd::new(21), Fd::new(22)]);
    let mut spawned = rig.complete(spawn, Ok(Done::Spawned { pidfd: Fd::new(20) }));
    let wait = spawned.take(Kind::Wait);
    let (input, output_pipe) = match spawned.events.as_slice() {
        [Event::Spawned { pipes, .. }] => match pipes.as_ref() {
            [input, output_pipe] => (*input, *output_pipe),
            [] | [_] | [_, _, _, ..] => panic!("both actual pipes announced"),
        },
        other => panic!("actual Spawned: {other:?}"),
    };
    let read = rig.next().take(Kind::PipeRead);
    output(&mut rig, output_pipe, OutputDown::Room { right: WRONG, bytes: 8 }).nothing();
    rig.next().nothing();
    output(&mut rig, input, OutputDown::Room { right: FIRST, bytes: 8 }).nothing();
    assert_eq!(
        rig.next().events,
        [Event::Output { owner: input, up: OutputUp::Settled { right: FIRST, outcome: OutputOutcome::Granted } }]
    );
    let write = output(&mut rig, input, OutputDown::Send { right: FIRST, bytes: Box::from(&b"response"[..]) })
        .take(Kind::PipeWrite);
    output(&mut rig, input, OutputDown::Room { right: SECOND, bytes: 1 }).nothing();
    rig.next().nothing();
    assert_eq!(
        rig.complete(wait, Ok(Done::Exit(crate::kernel::Exit::Code(0)))).events,
        [Event::Exited { owner: owner(1), exit: crate::kernel::Exit::Code(0) }],
        "the observed exit keeps both actual pipe lifetimes owned"
    );
    rig.down(Request::Close { entity: input }).nothing();
    assert_eq!(
        rig.next().events,
        [Event::Output { owner: input, up: OutputUp::Settled { right: SECOND, outcome: OutputOutcome::Cancelled } }]
    );
    let continued = rig.complete(write, Ok(Done::Count(3))).take(Kind::PipeWrite);
    match &continued.kind {
        Op::PipeWrite { bytes, from, .. } => assert_eq!((bytes.as_ref(), *from), (&b"response"[..], 3)),
        other @ (Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Sync { .. }
        | Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::Cancel { .. }) => panic!("actual continued PipeWrite: {other:?}"),
    }
    let input_close = rig.complete(continued, Ok(Done::Count(5))).take(Kind::Close);
    assert_eq!(rig.complete(input_close, Ok(Done::Nothing)).events, [Event::Closed { owner: input }]);
    assert_eq!(rig.complete(read, Ok(Done::Count(0))).events, [Event::Stream { owner: output_pipe, up: Up::End }]);
    let output_close = rig.down(Request::Close { entity: output_pipe }).take(Kind::Close);
    let mut closed = rig.complete(output_close, Ok(Done::Nothing));
    assert_eq!(closed.events, [Event::Closed { owner: output_pipe }]);
    let killing = closed.take(Kind::Signal);
    assert_eq!(
        killing.kind,
        Op::Signal { pidfd: Fd::new(20), signal: crate::kernel::Signal::Kill, to: crate::kernel::Target::Group }
    );
    let reaping = rig.complete(killing, Ok(Done::Nothing)).take(Kind::Wait);
    assert_eq!(reaping.kind, Op::Wait { pidfd: Fd::new(20), reap: true });
    let pidfd_close = rig.complete(reaping, Ok(Done::Exit(crate::kernel::Exit::Code(0)))).take(Kind::Close);
    assert_eq!(pidfd_close.kind, Op::Close { fd: Fd::new(20) });
    assert_eq!(rig.complete(pidfd_close, Ok(Done::Nothing)).events, [Event::Closed { owner: owner(1) }]);
    rig.next().nothing();
    rig.empty();
}
