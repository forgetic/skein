//! Independent common wire literals and malformed/one-over receiving controls.
use skein_channel::{
    Event, Fault, FrameWriter, LowerEvent, Opening, OpeningMode, Phase, Refusal, Request, Role, Schema, Step, framing,
};
use skein_channel_world::machine::{
    Harness, INITIATOR_TERMS, KNOWN, RESPONDER_TERMS, RULES, limits, literal, open_body, schema,
};
use skein_lib::{Token, stream};

#[test]
fn header_is_exact_eight_be_bytes_and_reserved_zero() {
    assert_eq!(framing(&[1, 2, 0, 0, 0, 0, 1, 3]).expect("literal header").body_bytes, 259);
    for size in 0..8 {
        assert!(framing(&[0; 8][..size]).is_none());
    }
    assert!(framing(&[0; 9]).is_none());
    assert!(framing(&[1, 2, 0, 1, 0, 0, 0, 0]).is_none());
}

#[test]
fn common_open_accept_terms_refusal_ping_status_match_independent_bytes() {
    let mut client = Harness::new(schema(Role::Initiator, OpeningMode::AskParent, false, 1));
    client.grant();
    client.request(Request::Open {
        owner: Token::new(91),
        opening: Opening {
            channel: 1,
            lowest: 1,
            highest: 1,
            name: Box::from(&b"n"[..]),
            secret: Box::from(&b"s"[..]),
        },
    });
    assert_eq!(
        client.sent[0].as_ref(),
        &[0, 1, 0, 0, 0, 0, 0, 19, b't', b'm', b'p', b'r', 1, 0, 1, 0, 1, 0, 0, 0, 1, b'n', 0, 0, 0, 1, b's']
    );
    assert!(matches!(client.events.pop(), Some(Event::Sent { owner }) if owner == Token::new(91)));
    client.feed(&literal(2, &[0, 1]));
    client.grant();
    assert_eq!(client.sent[1].as_ref(), literal(16, INITIATOR_TERMS).as_ref());
    client.feed(&literal(16, RESPONDER_TERMS));
    assert!(matches!(client.events.pop(), Some(Event::Ready { version: 1 })));
    client.grant();
    client.request(Request::Ping { owner: Token::new(92) });
    assert_eq!(client.sent[2].as_ref(), &[0, 4, 0, 0, 0, 0, 0, 0]);
    client.events.pop().expect("Ping Sent");
    client.grant();
    client.request(Request::Refuse { refusal: Refusal { reason: 65530, text: Box::from(&b"no"[..]) } });
    assert_eq!(client.sent[3].as_ref(), &[0, 3, 0, 0, 0, 0, 0, 8, 255, 250, 0, 0, 0, 2, b'n', b'o']);
    assert!(client.finished);
}

#[test]
fn terms_keep_original_receive_row_order_and_ready_precedes_transmission() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AcceptHighest, true, 1));
    server.feed(&literal(1, &open_body(0, 1)));
    assert!(server.sent.is_empty(), "no genuine grant yet");
    server.feed(&literal(16, INITIATOR_TERMS));
    assert!(matches!(server.events.pop(), Some(Event::Ready { version: 1 })));
    assert_eq!(server.machine.pending_bytes(), 34, "Accept plus source-order Terms admitted");
    server.grant();
    assert_eq!(server.sent[0].as_ref(), &[0, 2, 0, 0, 0, 0, 0, 2, 0, 1]);
    server.grant();
    assert_eq!(server.sent[1].as_ref(), literal(16, RESPONDER_TERMS).as_ref());
}

#[test]
fn ask_parent_pauses_and_peer_offer_can_extend_outside_local_range() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, true, 1));
    server.feed(&literal(1, &open_body(0, 99)));
    assert!(
        matches!(server.events.pop(), Some(Event::Opening { opening }) if opening.lowest == 0 && opening.highest == 99)
    );
    assert_eq!(server.machine.phase(), Phase::Authorizing);
    assert!(server.read.is_none());
    server.request(Request::Accept { version: 1 });
    assert_eq!(server.machine.phase(), Phase::Terms);
}

#[test]
fn disjoint_offer_is_version_reversed_is_framing() {
    for (lowest, highest, expected) in [(2, 99, Fault::Version), (2, 1, Fault::Framing)] {
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
        server.feed(&literal(1, &open_body(lowest, highest)));
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault }) if fault == expected));
    }
}

#[test]
fn maximum_open_and_refusal_use_frozen_actual_sizes() {
    let mut client = Harness::new(schema(Role::Initiator, OpeningMode::AskParent, false, 1));
    client.grant();
    client.request(Request::Open {
        owner: Token::new(1),
        opening: Opening {
            channel: 1,
            lowest: 1,
            highest: 1,
            name: vec![b'n'; 64].into_boxed_slice(),
            secret: vec![b's'; 64].into_boxed_slice(),
        },
    });
    assert_eq!(client.sent[0].len(), 153, "145 actual body under frozen 256 cap");
    client.events.pop().expect("Open Sent");
    client.grant();
    client.request(Request::Refuse { refusal: Refusal { reason: 40000, text: vec![b'x'; 506].into_boxed_slice() } });
    assert_eq!(client.sent[1].len(), 520);
}

#[test]
fn raw_common_body_truncation_trailing_counts_and_one_over_are_rejected() {
    let body = open_body(1, 1);
    for cut in 0..body.len() {
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
        server.feed(&literal(1, &body[..cut]));
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
        assert_eq!(server.machine.received_frames(), 0);
    }
    let mut trailing = body.to_vec();
    trailing.push(0);
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
    server.feed(&literal(1, &trailing));
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
    for terms in [vec![0, 0, 0, 33], vec![0, 0, 0, 1], vec![0, 0, 0, 0, 0]] {
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AcceptHighest, false, 1));
        server.feed(&literal(1, &body));
        server.feed(&literal(16, &terms));
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
    }
}

#[test]
fn foreign_and_newer_wrong_direction_never_become_unknown_skips() {
    for kind in [641, 385, 392] {
        let mut server = Harness::ready_responder(false);
        let step = server.bytes(&literal(kind, &[]));
        assert_eq!(step, Step::Halt);
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
    }
    let mut server = Harness::ready_responder(false);
    server.feed(&literal(264, &[7; 16]));
    assert_eq!(server.machine.received_frames(), 3, "newer legal direction skips once");
    server.grant();
    assert_eq!(server.sent.last().expect("status moved").as_ref(), &[0, 17, 0, 0, 0, 0, 0, 2, 1, 8]);
}

#[test]
fn bad_terms_duplicate_foreign_missing_small_bound_close_limits() {
    for terms in [
        vec![0, 0, 0, 2, 1, 129, 0, 0, 0, 64, 1, 129, 0, 0, 0, 64],
        vec![0, 0, 0, 1, 2, 129, 0, 0, 2, 88],
        vec![0, 0, 0, 1, 1, 129, 0, 0, 0, 64],
        vec![0, 0, 0, 2, 1, 129, 0, 0, 0, 63, 1, 130, 0, 0, 0, 32],
    ] {
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AcceptHighest, false, 1));
        server.feed(&literal(1, &open_body(1, 1)));
        server.feed(&literal(16, &terms));
        assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Limits })));
        assert_eq!(server.machine.received_frames(), 2, "fully decoded Terms before semantic failure");
    }
}

#[test]
fn schema_rejects_overrides_duplicates_noncontiguous_unknown_cap_and_overflow() {
    let base = schema(Role::Responder, OpeningMode::AskParent, false, 1);
    let version = skein_channel::VersionRule {
        version: 1,
        unknown_body_bytes: 600,
        first: skein_channel::First { receive: None, send: None, initial_read: true, ping_before_receive: false },
    };
    let profile = *base.profile();
    let mut known = KNOWN;
    known[0].kind = 16;
    assert!(Schema::new(Role::Responder, profile, limits(), &known, &RULES[..5], &[version]).is_none());
    let mut rules = RULES[..5].to_vec();
    rules[1] = rules[0];
    assert!(Schema::new(Role::Responder, profile, limits(), &KNOWN, &rules, &[version]).is_none());
    let bad = skein_channel::VersionRule { unknown_body_bytes: 512, ..version };
    assert!(Schema::new(Role::Responder, profile, limits(), &KNOWN, &RULES[..5], &[bad]).is_none());
    let mut rules = RULES[..5].to_vec();
    rules[0].body_bytes = u32::MAX;
    assert!(Schema::new(Role::Responder, profile, limits(), &KNOWN, &rules, &[version]).is_none());
    let bad = skein_channel::VersionRule { version: 3, ..version };
    assert!(Schema::new(Role::Responder, profile, limits(), &KNOWN, &RULES, &[version, bad]).is_none());
}

#[test]
fn producer_short_and_one_over_are_data_and_no_intermediate_body_copy() {
    let schema = schema(Role::Responder, OpeningMode::AskParent, false, 1);
    assert!(FrameWriter::new(&schema, 1, 385, 65).is_none());
    assert!(FrameWriter::new(&schema, 1, 257, 0).is_none());
    let mut writer = FrameWriter::new(&schema, 1, 385, 2).expect("fits measured size");
    writer.put(&[9]).expect("first byte");
    assert!(writer.put(&[0, 0]).is_none());
    assert!(writer.finish().is_none(), "short encode fails without finish panic");
}

#[test]
fn opaque_received_refusal_is_delivered_before_logical_close() {
    let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
    server.feed(&[0, 3, 0, 0, 0, 0, 0, 8, 255, 250, 0, 0, 0, 2, b'n', b'o']);
    assert!(matches!(server.events.pop(), Some(Event::Refused { reason:65530,text }) if text.as_ref()==b"no"));
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Closed })));
    assert!(!server.machine.is_retired());
    skein_channel::up(&mut server.machine, LowerEvent::Closed, &mut server.events, &mut server.lower);
    assert!(!server.machine.is_retired(), "anonymous Closed cannot settle named pending right");
    server.grant();
    assert!(server.machine.is_retired());
    assert!(server.events.is_empty());
}

#[test]
fn classic_room_cannot_impersonate_native_credit() {
    let mut server = Harness::ready_responder(false);
    let step = skein_channel::up(
        &mut server.machine,
        LowerEvent::Stream(stream::Up::Room),
        &mut server.events,
        &mut server.lower,
    );
    assert_eq!(step, Step::Halt);
    assert!(matches!(server.events.pop(), Some(Event::Closed { fault: Fault::Framing })));
}
