//! Actual heap controls: direct final encoding and raw+decoded+encoded overlap.
use skein_channel::{Event, FrameWriter, OpeningMode, Request, Role, Step};
use skein_channel_world::machine::{Harness, literal, schema};
use skein_heap::{Counting, Meter, Span};
use skein_lib::Token;

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn exact_final_encoder_allocates_only_header_plus_measured_body() {
    let schema = schema(Role::Responder, OpeningMode::AskParent, false, 1);
    let span = Span::start();
    let mut writer = FrameWriter::new(&schema, 1, 385, 64).expect("maximum local sender body");
    writer.put(&[9; 64]).expect("write directly into final allocation");
    let encoded = writer.finish().expect("seal full exact final allocation");
    let growth = span.end();
    assert_eq!(growth.peak, 72, "one final frame, no body-then-frame allocation");
    assert_eq!(growth.net, 72);
    assert_eq!(encoded.bytes().len(), 72);
    drop(encoded);
}

#[test]
fn schema_fixed_queue_raw_delivery_and_exact_scratch_stay_within_declared_bound() {
    for chunk in [1, 7, 600] {
        let base = schema(Role::Responder, OpeningMode::AskParent, false, 1);
        let mut limits = *base.limits();
        limits.chunk_bytes = chunk;
        let versions = [skein_channel::VersionRule {
            version: 1,
            unknown_body_bytes: 600,
            first: skein_channel::First { receive: None, send: None, initial_read: true, ping_before_receive: false },
        }];
        let profile = *base.profile();
        drop(base);
        let meter = Meter::new();
        meter.start();
        let source = skein_channel::Schema::new(
            Role::Responder,
            profile,
            limits,
            &skein_channel_world::machine::KNOWN,
            &skein_channel_world::machine::RULES[..5],
            &versions,
        )
        .expect("varied actual chunk schema");
        let own = source.worst_case().expect("checked heap bound");
        let mut machine = skein_channel::Machine::new(source);
        let mut events = skein_lib::Queue::with_capacity(2);
        let mut lower = skein_lib::Queue::with_capacity(3);
        skein_channel::poll(&mut machine, &mut events, &mut lower);
        let measured = meter.end();
        meter.check(measured, own, &chunk);
        assert!(meter.held() <= own);
        drop(machine);
        drop(events);
        drop(lower);
        assert_eq!(meter.held(), 0);
    }
}

#[test]
fn actual_raw_decoded_array_and_outgoing_final_frame_coexist_by_checked_sum() {
    let mut server = Harness::ready_responder(false);
    let frame = literal(257, &[9; 64]);
    let step = server.feed(&frame);
    assert!(matches!(step, Step::NeedDecode { .. }));
    let event = server.events.pop().expect("raw Body");
    let raw = match event {
        Event::Body { bytes, .. } => bytes,
        event => panic!("unexpected {event:?}"),
    };
    let span = Span::start();
    // Represents the wrapper's actual bounded typed array and field allocation;
    // both stay owned while the producer writes its final outgoing frame.
    let decoded = raw.to_vec().into_boxed_slice();
    let mut writer = FrameWriter::new(server.machine.schema(), 1, 385, 64).expect("maximum sender");
    writer.put(&decoded).expect("final output from typed wrapper field");
    let encoded = writer.finish().expect("full final frame");
    let measured = span.end();
    assert_eq!(measured.peak, 136, "64 decoded plus 72 exact frame, while raw64 remains owned");
    assert_eq!(measured.net, 136);
    let aggregate = 64_u64.checked_add(64).expect("raw+decoded").checked_add(72).expect("raw+decoded+encoded");
    assert_eq!(aggregate, 200);
    assert_eq!(
        raw.len() + decoded.len() + encoded.bytes().len(),
        usize::try_from(aggregate).expect("bounded aggregate")
    );
    drop(raw);
    drop(decoded);
    drop(encoded);
}

#[test]
fn owned_output_byte_and_slot_maxima_are_real_and_rejected_frames_remain_caller_owned() {
    let mut server = Harness::ready_responder(false);
    let bound = server.machine.schema().worst_case().expect("checked bound");
    let meter = Meter::new();
    for owner in 0..4 {
        meter.start();
        server.send(385, &[5; 64], owner);
        server.events.pop().expect("Sent");
        let measured = meter.end();
        meter.check(measured, bound, &owner);
    }
    assert_eq!(server.machine.pending_bytes(), 288);
    let mut writer = FrameWriter::new(server.machine.schema(), 1, 385, 64).expect("caller producer owns input frame");
    writer.put(&[5; 64]).expect("exact body");
    let encoded = writer.finish().expect("final caller frame");
    let span = Span::start();
    server.request(Request::Send { owner: Token::new(9), encoded: Some(encoded) });
    let measured = span.end();
    assert!(measured.net < 0, "slot refusal drops owned rejected frame and old queue");
}

#[test]
fn actual_maximum_common_raw_delivery_and_decoded_refusal_coexist_under_bound() {
    let base = schema(Role::Responder, OpeningMode::AskParent, false, 1);
    let mut limits = *base.limits();
    limits.chunk_bytes = 600;
    let profile = *base.profile();
    drop(base);
    let versions = [skein_channel::VersionRule {
        version: 1,
        unknown_body_bytes: 600,
        first: skein_channel::First { receive: None, send: None, initial_read: true, ping_before_receive: false },
    }];
    let meter = Meter::new();
    meter.start();
    let schema = skein_channel::Schema::new(
        Role::Responder,
        profile,
        limits,
        &skein_channel_world::machine::KNOWN,
        &skein_channel_world::machine::RULES[..5],
        &versions,
    )
    .expect("maximum common schema");
    let bound = schema.worst_case().expect("checked bound");
    let mut machine = skein_channel::Machine::new(schema);
    let mut events = skein_lib::Queue::with_capacity(2);
    let mut lower = skein_lib::Queue::with_capacity(3);
    skein_channel::poll(&mut machine, &mut events, &mut lower);
    while lower.pop().is_some() {}
    let header = Box::from([0, 3, 0, 0, 0, 0, 2, 0]);
    assert_eq!(
        skein_channel::up(
            &mut machine,
            skein_channel::LowerEvent::Stream(skein_lib::stream::Up::Bytes(header)),
            &mut events,
            &mut lower
        ),
        Step::NeedPoll
    );
    skein_channel::poll(&mut machine, &mut events, &mut lower);
    while lower.pop().is_some() {}
    let mut literal = [b'x'; 512];
    literal[..6].copy_from_slice(&[255, 250, 0, 0, 1, 250]);
    let delivered = Box::from(literal);
    assert_eq!(
        skein_channel::up(
            &mut machine,
            skein_channel::LowerEvent::Stream(skein_lib::stream::Up::Bytes(delivered)),
            &mut events,
            &mut lower
        ),
        Step::Halt
    );
    let measured = meter.end();
    meter.check(measured, bound, &"maximum raw512+delivery512+decoded506");
    assert!(measured.peak() > 506, "real common decode allocated actual maximum text");
    let refusal = events.pop().expect("peer Refused");
    assert!(matches!(refusal,Event::Refused {reason:65530,text} if text.len()==506));
    while events.pop().is_some() {}
    while lower.pop().is_some() {}
    drop(machine);
    drop(events);
    drop(lower);
    assert_eq!(meter.held(), 0);
}
