//! Independent receipt, capacity, no-spin and real-lower-lifetime controls.
use skein_channel::{Disposition, Event, Fault, LowerEvent, OpeningMode, Phase, Request, Role, Step};
use skein_channel_world::machine::{Harness, literal, schema};
use skein_lib::{Token, stream};

#[test]
fn automatic_first_body_uses_original_read_and_pause_requires_next_read() {
    let mut server = Harness::ready_responder(true);
    let step = server.feed(&literal(257, &[9, 8, 7]));
    let receipt = match step {
        Step::NeedDecode { receipt } => receipt,
        Step::NeedPoll | Step::Halt => panic!("known body needs decode"),
    };
    let event = server.events.pop().expect("one raw Body");
    assert!(matches!(&event, Event::Body { kind:257,version:1,bytes,.. } if bytes.as_ref()==[9,8,7]));
    assert!(server.read.is_none());
    server.request(Request::Read);
    assert!(server.read.is_none(), "early Read cannot overwrite AwaitDecode");
    server.request(Request::Resolve { receipt: Token::new(999), disposition: Disposition::DecodedContinue });
    assert!(server.read.is_none(), "stale receipt cannot rearm input");
    server.request(Request::Resolve { receipt, disposition: Disposition::DecodedPause });
    assert!(server.read.is_none(), "already permitted body publishes before pause");
    assert_eq!(server.machine.received_frames(), 3);
    server.request(Request::Read);
    assert_eq!(server.read, Some(8));
    drop(event);
}

#[test]
fn invalid_body_and_decoded_phase_reject_have_distinct_counter_stage() {
    for (disposition, count) in [(Disposition::InvalidBody, 2), (Disposition::DecodedReject, 3)] {
        let mut server = Harness::ready_responder(false);
        let step = server.feed(&literal(258, &[]));
        let receipt = match step {
            Step::NeedDecode { receipt } => receipt,
            Step::NeedPoll | Step::Halt => panic!("known receipt"),
        };
        server.events.pop().expect("raw Body");
        server.request(Request::Resolve { receipt, disposition });
        assert_eq!(server.machine.received_frames(), count);
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
        assert_eq!(server.request(Request::Resolve { receipt, disposition: Disposition::DecodedContinue }), Step::Halt);
        assert!(server.events.is_empty());
    }
}

#[test]
fn unsupported_is_parent_delivery_and_pauses_input() {
    let mut server = Harness::ready_responder(false);
    server.feed(&[0, 17, 0, 0, 0, 0, 0, 2, 18, 52]);
    assert!(matches!(server.events.pop(), Some(Event::Unsupported { kind: 4660 })));
    assert!(server.read.is_none());
    server.request(Request::Read);
    assert_eq!(server.read, Some(8));
}

#[test]
fn first_gate_rejects_ordinary_unknown_and_wrong_first_before_allocation() {
    for kind in [258, 1234, 17] {
        let mut server = Harness::ready_responder(true);
        server.bytes(&literal(kind, &[]));
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
        assert_eq!(server.machine.received_frames(), 2);
    }
    let mut server = Harness::ready_responder(true);
    server.feed(&[0, 4, 0, 0, 0, 0, 0, 0]);
    assert!(server.events.is_empty());
    assert_eq!(server.read, Some(8), "allowed Ping preserves first gate");
}

#[test]
fn whole_cap_credit_debt_resets_only_on_matching_real_grant() {
    let mut server = Harness::ready_responder(false);
    assert_eq!(server.machine.pending_bytes(), 0);
    assert_eq!(server.machine.queued_bytes(), 24);
    let current = server.pending.expect("eager replacement Room");
    let step = skein_channel::up(
        &mut server.machine,
        LowerEvent::Output(stream::OutputUp::Settled {
            right: Token::new(999),
            outcome: stream::OutputOutcome::Granted,
        }),
        &mut server.events,
        &mut server.lower,
    );
    assert_eq!(step, Step::Halt);
    assert_eq!(server.machine.queued_bytes(), 24);
    server.send(385, &[1, 2], 77);
    assert!(matches!(server.events.pop(), Some(Event::Sent { .. })));
    assert_eq!(server.machine.pending_bytes(), 10);
    assert_eq!(server.pending, Some(current), "Send cannot consume unanswered native right");
    assert_eq!(server.machine.queued_bytes(), 34);
    server.grant();
    assert_eq!(server.machine.queued_bytes(), 10, "only real grant resets old S before move");
    assert_eq!(server.machine.pending_bytes(), 0);
    assert_eq!(server.sent.last().expect("actual frame").as_ref(), &[1, 129, 0, 0, 0, 0, 0, 2, 1, 2]);
    assert_eq!(server.machine.room(), 1014, "logical room is not physical grant remainder");
    assert!(server.granted.is_none());
    assert!(server.pending.is_some());
    assert!(!server.machine.is_ready(), "pending Room plus live read never spins");
}

#[test]
fn actual_queue_slots_and_bytes_are_independent_admission_failures() {
    let mut server = Harness::ready_responder(false);
    for owner in 0..4 {
        server.send(385, &[0], owner);
        server.events.pop().expect("Sent");
    }
    assert_eq!(server.machine.pending_bytes(), 36);
    server.send(385, &[0], 5);
    assert!(matches!(server.events.pop(), Some(Event::Unsent { owner }) if owner==Token::new(5)));
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::OutputFull })));
    assert!(server.machine.room() == 0);
}

#[test]
fn unknown_flood_holds_one_kind_until_status_slot_admission_then_resumes() {
    let mut server = Harness::ready_responder(false);
    for owner in 0..4 {
        server.send(385, &[], owner);
        server.events.pop().expect("Sent");
    }
    server.feed(&literal(4000, &[7; 600]));
    assert!(server.read.is_none(), "one skipped kind waits for actual queue slot");
    assert_eq!(server.machine.received_frames(), 2);
    assert!(!server.machine.is_ready(), "full slot queue waiting on native right is blocked");
    server.grant();
    assert!(server.read.is_none(), "one poll sends at most one and status was still full at preflight");
    assert!(server.machine.is_ready(), "released local slot makes status admission runnable");
    server.poll();
    assert_eq!(server.machine.received_frames(), 3);
    assert_eq!(server.read, Some(8));
    assert_eq!(server.machine.pending_bytes(), 34, "three old frames and one status, bounded Q");
}

#[test]
fn unknown_one_over_global_all_kind_max_closes_before_skip() {
    let mut server = Harness::ready_responder(false);
    server.bytes(&[15, 160, 0, 0, 0, 0, 2, 89]);
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
}

#[test]
fn logical_stop_keeps_identity_until_late_grant_then_actual_resource_closed() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
    let pending = server.pending.expect("initial native Room");
    server.request(Request::Close);
    assert_eq!(server.pending, Some(pending));
    assert!(!server.finished, "Waiting retirement defers Finish");
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Closed })));
    server.grant();
    assert!(server.finished, "late actual grant Released then Finish");
    assert!(server.events.is_empty());
    assert!(!server.machine.is_retired());
    skein_channel::up(&mut server.machine, LowerEvent::Closed, &mut server.events, &mut server.lower);
    assert!(server.machine.is_retired());
}

#[test]
fn fatal_refusal_requires_actual_observed_grant_and_keeps_empty_fourteen_bytes() {
    for observed in [false, true] {
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
        if observed {
            server.grant();
        }
        server.bytes(&[0, 1, 0, 1, 0, 0, 0, 0]);
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
        if observed {
            assert_eq!(server.sent.len(), 1);
            assert_eq!(server.sent[0].as_ref(), &[0, 3, 0, 0, 0, 0, 0, 6, 0, 6, 0, 0, 0, 0]);
        } else {
            assert!(server.sent.is_empty());
            server.grant();
            assert!(server.sent.is_empty(), "queued grant cannot retroactively justify refusal");
        }
    }
}

#[test]
fn approved_input_eof_drops_partial_body_and_later_output_still_progresses() {
    let mut server = Harness::ready_responder(false);
    server.bytes(&[1, 1, 0, 0, 0, 0, 0, 10]);
    server.bytes(&[1; 7]);
    let before = server.sent.len();
    let step = server.machine.read_end(&mut server.events, &mut server.lower);
    assert_eq!(step, Step::Halt);
    server.drain();
    assert!(matches!(server.events.pop(), Some(Event::ReadEnded)));
    assert!(server.read.is_none());
    assert_eq!(server.sent.len(), before, "EOF does not sneak an output poll");
    server.send(385, b"late", 77);
    server.events.pop().expect("late output admitted");
    server.grant();
    assert_eq!(server.sent.last().expect("late output").as_ref(), literal(385, b"late").as_ref());
    assert_eq!(server.machine.phase(), Phase::Ready);
}

#[test]
fn live_none_closes_unsent_then_outputfull_but_stopped_none_is_unsent_only() {
    let mut server = Harness::ready_responder(false);
    server.request(Request::Send { owner: Token::new(99), encoded: None });
    assert!(matches!(server.events.pop(), Some(Event::Unsent { owner }) if owner==Token::new(99)));
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::OutputFull })));
    server.request(Request::Send { owner: Token::new(100), encoded: None });
    assert!(matches!(server.events.pop(), Some(Event::Unsent { owner }) if owner==Token::new(100)));
    assert!(server.events.is_empty());
}

#[test]
fn service_payload_version_mismatch_is_unsent_without_encoding_failure_close() {
    let mut server = Harness::ready_responder(false);
    let producer = schema(Role::Responder, OpeningMode::AskParent, false, 2);
    let mut writer = skein_channel::FrameWriter::new(&producer, 2, 385, 1).expect("V2 producer schema");
    writer.put(&[9]).expect("measured body");
    let encoded = writer.finish().expect("V2 payload");
    assert_eq!(server.request(Request::Send { owner: Token::new(81), encoded: Some(encoded) }), Step::Halt);
    assert!(matches!(server.events.pop(),Some(Event::Unsent {owner}) if owner==Token::new(81)));
    assert!(server.events.is_empty());
    assert_eq!(server.machine.phase(), Phase::Ready);
    assert_eq!(server.machine.pending_bytes(), 0);
}

#[test]
fn received_ordinary_finish_like_body_can_pause_then_read_late_stdout() {
    let mut server = Harness::ready_responder(false);
    for body in [b"final".as_slice(), b"late stdout".as_slice()] {
        let step = server.feed(&literal(258, body));
        let receipt = match step {
            Step::NeedDecode { receipt } => receipt,
            Step::NeedPoll | Step::Halt => panic!("raw receipt"),
        };
        assert!(matches!(server.events.pop(),Some(Event::Body {bytes,..}) if bytes.as_ref()==body));
        server.request(Request::Resolve { receipt, disposition: Disposition::DecodedPause });
        assert!(server.read.is_none());
        assert_eq!(server.machine.phase(), Phase::Ready, "shared never guesses service final meaning");
        server.request(Request::Read);
    }
}

#[test]
fn normal_final_one_poll_is_withdraw_last_send_finish_and_no_replacement_room() {
    let mut server = Harness::ready_responder(false);
    server.grant();
    let mut writer = skein_channel::FrameWriter::new(server.machine.schema(), 1, 385, 1).expect("sender schema");
    writer.put(&[9]).expect("exact body");
    let encoded = writer.finish().expect("frame");
    assert_eq!(
        skein_channel::down(
            &mut server.machine,
            Request::Send { owner: Token::new(82), encoded: Some(encoded) },
            &mut server.events,
            &mut server.lower
        ),
        Step::NeedPoll
    );
    assert!(matches!(server.events.pop(), Some(Event::Sent { .. })));
    assert_eq!(
        skein_channel::down(&mut server.machine, Request::Finish, &mut server.events, &mut server.lower),
        Step::NeedPoll
    );
    skein_channel::poll(&mut server.machine, &mut server.events, &mut server.lower);
    assert_eq!(server.lower.len(), 3, "last Send/read withdrawal/Finish only");
    server.drain();
    assert!(server.pending.is_none());
    assert!(server.finished);
    assert_eq!(server.machine.phase(), Phase::Closing);
}

#[test]
fn missing_output_terminal_survives_resource_closed_until_actual_cancelled() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
    let right = server.pending.expect("actual admitted Room");
    server.request(Request::Close);
    server.events.pop().expect("logical Closed");
    skein_channel::up(&mut server.machine, LowerEvent::Closed, &mut server.events, &mut server.lower);
    assert!(!server.machine.is_retired());
    server.pending = None;
    let step = skein_channel::up(
        &mut server.machine,
        LowerEvent::Output(stream::OutputUp::Settled { right, outcome: stream::OutputOutcome::Cancelled }),
        &mut server.events,
        &mut server.lower,
    );
    assert_eq!(step, Step::Halt);
    assert!(server.machine.is_retired());
    assert!(server.events.is_empty());
    assert!(server.lower.is_empty(), "no Finish after actual resource closure");
}

#[test]
fn unsolicited_actual_output_cancelled_closes_without_creating_reusable_credit() {
    let mut server = Harness::ready_responder(false);
    let right = server.pending.take().expect("actual whole-cap debt Room");
    let step = skein_channel::up(
        &mut server.machine,
        LowerEvent::Output(stream::OutputUp::Settled { right, outcome: stream::OutputOutcome::Cancelled }),
        &mut server.events,
        &mut server.lower,
    );
    assert_eq!(step, Step::Halt);
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Closed })));
    server.drain();
    assert_eq!(server.machine.room(), 0);
    assert!(!server.machine.is_ready());
}

#[test]
fn byte_pressure_rejects_while_real_frame_slots_remain_available() {
    let base = schema(Role::Responder, OpeningMode::AcceptHighest, false, 1);
    let mut rules = skein_channel_world::machine::RULES[..5].to_vec();
    rules[2].body_bytes = 600;
    let versions = [skein_channel::VersionRule {
        version: 1,
        unknown_body_bytes: 600,
        first: skein_channel::First { receive: None, send: None, initial_read: true, ping_before_receive: false },
    }];
    let configured = skein_channel::Schema::new(
        Role::Responder,
        *base.profile(),
        *base.limits(),
        &skein_channel_world::machine::KNOWN,
        &rules,
        &versions,
    )
    .expect("large local sender within C");
    let mut server = Harness::new(configured);
    server.grant();
    server.feed(&literal(1, &skein_channel_world::machine::open_body(1, 1)));
    server.grant();
    server.feed(&literal(16, &[0, 0, 0, 2, 1, 129, 0, 0, 2, 88, 1, 130, 0, 0, 0, 32]));
    assert!(matches!(server.events.pop(), Some(Event::Ready { .. })));
    server.send(385, &[9; 600], 1);
    assert!(matches!(server.events.pop(), Some(Event::Sent { .. })));
    assert_eq!(server.machine.pending_bytes(), 608);
    server.send(385, &[9; 600], 2);
    assert!(matches!(server.events.pop(), Some(Event::Unsent { .. })));
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::OutputFull })));
}

#[test]
fn retained_idle_grant_is_not_runnable_work_and_does_not_issue_second_room() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
    server.grant();
    assert!(server.granted.is_some() && server.pending.is_none());
    assert!(!server.machine.is_ready());
    server.poll();
    assert!(server.pending.is_none());
    assert!(server.granted.is_some());
}

#[test]
fn approved_actual_eof_at_partial_header_skip_and_empty_body_never_polls() {
    for kind in [None, Some(257), Some(4000)] {
        let mut server = Harness::ready_responder(false);
        if let Some(kind) = kind {
            server.bytes(&literal(kind, &[0; 10])[..8]);
        }
        let before = server.machine.queued_bytes();
        assert_eq!(server.machine.read_end(&mut server.events, &mut server.lower), Step::Halt);
        server.drain();
        assert!(matches!(server.events.pop(), Some(Event::ReadEnded)));
        assert_eq!(server.machine.queued_bytes(), before, "EOF cannot reset native admission debt");
        assert!(server.read.is_none());
    }
}
