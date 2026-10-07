//! Hand-written bytes for codec.md, sections 3-4.

#[cfg(test)]
mod tests {

    use skein_codec::Reason;
    use skein_codec_tests::scalars::{CEILINGS as SCALAR_CEILINGS, Flag, Scalars, ScalarsParts};
    use skein_codec_tests::v1::{CEILINGS, Choice, Effect, Field, FieldParts, Path, Report, ReportParts};
    use skein_lib::{Duration, List, Reader, Writer};

    fn report() -> Report {
        let field = Field::new(&CEILINGS, FieldParts { name: Box::from(&b"A"[..]), value: Box::from(&[0x7f][..]) })
            .expect("within ceilings");
        let mut fields = List::with_capacity(1);
        fields.push(field).expect("room");
        Report::new(
            &CEILINGS,
            ReportParts {
                title: Box::from(&b"R"[..]),
                fields,
                effect: Effect::Write,
                present: Some(true),
                nonce: [1, 2],
                age: Duration::from_nanos(5),
            },
        )
        .expect("within ceilings")
    }

    const WIRE: [u8; 34] = [
        0, 1, // version
        0, 0, 0, 1, b'R', // title
        0, 0, 0, 1, // one field
        0, 0, 0, 1, b'A', // field name
        0, 0, 0, 1, 0x7f, // field value
        1,    // effect: write
        1, 1, // present: true
        1, 2, // nonce
        0, 0, 0, 0, 0, 0, 0, 5, // age
    ];

    #[test]
    fn a_generated_record_matches_hand_written_bytes() {
        let value = report();
        assert_eq!(value.measure(), 34);
        let mut writer = Writer::new(34);
        value.encode(&mut writer).expect("measured room");
        assert_eq!(&*writer.finish(), &WIRE);
        let decoded = Report::decode(&CEILINGS, &mut Reader::new(&WIRE)).expect("canonical bytes");
        assert_eq!(decoded, value);
        assert_eq!(decoded.into_parts().title.as_ref(), b"R");
    }

    #[test]
    fn noncanonical_and_malformed_bytes_have_typed_problems() {
        let mut bad_bool = WIRE;
        *bad_bool.get_mut(23).expect("option's bool") = 2;
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&bad_bool)).expect_err("bool beyond one");
        assert_eq!((problem.path, problem.reason), (Path::ReportPresent, Reason::Bool));

        let mut bad_tag = WIRE;
        *bad_tag.get_mut(21).expect("effect tag") = 2;
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&bad_tag)).expect_err("unknown enum tag");
        assert_eq!((problem.path, problem.reason), (Path::EffectTag, Reason::Tag));

        let mut bad_version = WIRE;
        *bad_version.get_mut(1).expect("version byte") = 2;
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&bad_version)).expect_err("wrong version");
        assert_eq!((problem.path, problem.reason), (Path::ReportVersion, Reason::Version));

        let problem =
            Report::decode(&CEILINGS, &mut Reader::new(WIRE.get(..33).expect("short prefix"))).expect_err("short age");
        assert_eq!((problem.path, problem.reason), (Path::ReportAge, Reason::Short));

        let mut trailing = Writer::new(35);
        trailing.put(&WIRE).expect("room");
        trailing.put(&[0]).expect("room");
        let trailing = trailing.finish();
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&trailing)).expect_err("trailing byte");
        assert_eq!((problem.path, problem.reason), (Path::ReportTail, Reason::Trailing));
    }

    #[test]
    fn bounds_and_utf8_are_checked_before_storing_fields() {
        let mut overlong = WIRE;
        *overlong.get_mut(14).expect("name length") = 5;
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&overlong)).expect_err("name beyond ceiling");
        assert_eq!((problem.path, problem.reason), (Path::FieldName, Reason::Bound));

        let mut invalid_utf8 = WIRE;
        *invalid_utf8.get_mut(15).expect("name byte") = 0xff;
        let problem = Report::decode(&CEILINGS, &mut Reader::new(&invalid_utf8)).expect_err("invalid UTF-8");
        assert_eq!((problem.path, problem.reason), (Path::FieldName, Reason::Utf8));
    }

    #[test]
    fn unbounded_scalar_family_uses_big_endian_and_canonical_tags() {
        const SCALARS_WIRE: [u8; 27] =
            [1, 2, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 0, 0, 0, 6, 7, 8, 1];
        let value = Scalars::new(
            &SCALAR_CEILINGS,
            ScalarsParts { a: 1, b: 0x0203, c: 4, d: 5, e: true, f: Duration::from_nanos(6), g: [7, 8], h: Flag::On },
        );
        assert_eq!(value.measure(), 27);
        let mut writer = Writer::new(27);
        value.encode(&mut writer).expect("measured room");
        assert_eq!(&*writer.finish(), &SCALARS_WIRE);
        assert_eq!(Scalars::decode(&SCALAR_CEILINGS, &mut Reader::new(&SCALARS_WIRE)), Ok(value));
    }

    #[test]
    fn variant_with_record_payload_has_tag_then_record_bytes() {
        let choice = Choice::Report(report());
        assert_eq!(choice.measure(), 35);
        let mut writer = Writer::new(35);
        choice.encode(&mut writer).expect("measured room");
        let wire = writer.finish();
        assert_eq!(wire.first(), Some(&0));
        assert_eq!(wire.get(1..), Some(WIRE.as_slice()));
        assert_eq!(Choice::decode(&CEILINGS, &mut Reader::new(&wire)), Ok(choice));

        let empty = Choice::Empty;
        let mut writer = Writer::new(1);
        empty.encode(&mut writer).expect("one byte");
        assert_eq!(&*writer.finish(), &[1]);
    }
}
