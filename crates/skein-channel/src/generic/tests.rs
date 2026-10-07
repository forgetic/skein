//! Independent literals for channel.md, sections 3–5.1.
use alloc::boxed::Box;
use skein_lib::List;

use super::{
    Control, Direction, FrameError, Kind, Limits, Schema, SchemaError, Term, Version, control_frame, decode_control,
    frame_writer, parse_header,
};

fn limits() -> Limits {
    Limits { chunk: 16, credential: 64, skip: 128, output_bytes: 1024, output_frames: 4, kinds: 4 }
}

fn schema() -> Schema {
    let mut kinds = List::with_capacity(2);
    kinds.push(Kind { kind: 0x0100, direction: Direction::FromInitiator, largest: 32 }).expect("kind fits");
    let mut versions = List::with_capacity(2);
    versions.push(Version { version: 1, kinds }).expect("version fits");
    let mut kinds = List::with_capacity(2);
    kinds.push(Kind { kind: 0x0101, direction: Direction::FromResponder, largest: 64 }).expect("kind fits");
    versions.push(Version { version: 2, kinds }).expect("version fits");
    Schema { magic: *b"test", versions }
}

#[test]
fn checked_schema_rejects_gaps_duplicates_reserved_and_oversize() {
    let mut source = schema();
    assert_eq!(source.check(&limits()), Ok(()));
    assert_eq!(source.range(), Some((1, 2)));
    assert_eq!(source.version(2).expect("version 2").kind(0x0101).expect("kind").largest, 64);
    source.versions.get_mut(1).expect("second version").version = 3;
    assert_eq!(source.check(&limits()), Err(SchemaError::Versions));
    source.versions.get_mut(1).expect("second version").version = 2;
    source
        .versions
        .get_mut(1)
        .expect("second version")
        .kinds
        .push(Kind { kind: 0x0101, direction: Direction::FromResponder, largest: 1 })
        .expect("second kind");
    assert_eq!(source.check(&limits()), Err(SchemaError::Kind));
    source.versions.get_mut(1).expect("second version").kinds.get_mut(1).expect("second kind").kind = 6;
    assert_eq!(source.check(&limits()), Err(SchemaError::Kind));
    source.versions.get_mut(1).expect("second version").kinds.get_mut(1).expect("second kind").kind = 0x0102;
    source.versions.get_mut(1).expect("second version").kinds.get_mut(1).expect("second kind").largest = 1017;
    assert_eq!(source.check(&limits()), Err(SchemaError::Kind));
}

#[test]
fn golden_six_control_frames_round_trip() {
    let mut terms = List::with_capacity(1);
    terms.push(Term { kind: 0x0100, largest: 32 }).expect("one term");
    let cases = [
        (
            Control::Open { magic: *b"test", lowest: 1, highest: 2, features: 0, credential: Box::from(*b"xy") },
            &[0, 1, 0, 0, 0, 0, 0, 16, 116, 101, 115, 116, 0, 1, 0, 2, 0, 0, 0, 0, 0, 2, 120, 121][..],
        ),
        (Control::Accept { version: 2, features: 0 }, &[0, 2, 0, 0, 0, 0, 0, 4, 0, 2, 0, 0][..]),
        (
            Control::Refuse { reason: 3, text: Box::from(*b"bad") },
            &[0, 3, 0, 0, 0, 0, 0, 9, 0, 3, 0, 0, 0, 3, 98, 97, 100][..],
        ),
        (Control::Terms { entries: terms }, &[0, 4, 0, 0, 0, 0, 0, 10, 0, 0, 0, 1, 1, 0, 0, 0, 0, 32][..]),
        (Control::Ping, &[0, 5, 0, 0, 0, 0, 0, 0][..]),
        (Control::Unsupported { kind: 0x0100 }, &[0, 6, 0, 0, 0, 0, 0, 2, 1, 0][..]),
    ];
    for (control, expected) in cases {
        let frame = control_frame(&control, &limits()).expect("control encodes");
        assert_eq!(frame.bytes(), expected);
        let header = parse_header(frame.bytes().get(..8).expect("header")).expect("valid header");
        assert_eq!(header.kind, frame.kind());
        assert_eq!(header.body_len, frame.body_len());
        let body = frame.bytes().get(8..).expect("body");
        let decoded = decode_control(header.kind, body, &limits()).expect("control decodes");
        let encoded_again = control_frame(&decoded, &limits()).expect("control reencodes");
        assert_eq!(encoded_again.bytes(), expected);
    }
}

#[test]
fn malformed_control_and_incomplete_writer_are_refused() {
    assert_eq!(parse_header(&[0, 5, 0, 1, 0, 0, 0, 0]), Err(FrameError::Malformed));
    assert_eq!(decode_control(4, &[0, 0, 0, 2], &limits()).err(), Some(FrameError::Malformed));
    assert_eq!(decode_control(3, &[0, 3, 0, 0, 1, 1], &limits()).err(), Some(FrameError::TooLarge));
    let mut writer = frame_writer(0x0100, 3).expect("measured frame");
    writer.put(b"ab").expect("two bytes fit");
    assert_eq!(writer.finish().err(), Some(FrameError::Incomplete));
}
