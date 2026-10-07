//! A decoded value at every ceiling attains its declared heap worst case
//! (codec.md, sections 4 and 6; testing-strategy.md, section 6).

use skein_heap::Counting;

#[global_allocator]
static HEAP: Counting = Counting;

#[cfg(test)]
mod tests {
    use skein_codec_tests::v1::{CEILINGS, Effect, Field, FieldParts, Report, ReportParts};
    use skein_heap::Meter;
    use skein_lib::{Duration, List, Reader, Writer};

    fn full_field() -> Field {
        Field::new(&CEILINGS, FieldParts { name: Box::from(&b"abcd"[..]), value: Box::from(&[1, 2, 3, 4][..]) })
            .expect("field at ceilings")
    }

    fn full_report() -> Report {
        let mut fields = List::with_capacity(CEILINGS.report_fields);
        fields.push(full_field()).expect("first field room");
        fields.push(full_field()).expect("second field room");
        Report::new(
            &CEILINGS,
            ReportParts {
                title: Box::from(&b"abcdefgh"[..]),
                fields,
                effect: Effect::Write,
                present: Some(true),
                nonce: [0xff, 0xff],
                age: Duration::from_nanos(u64::MAX),
            },
        )
        .expect("report at ceilings")
    }

    #[test]
    fn full_record_decoder_attains_and_stays_within_heap_worst_case() {
        let value = full_report();
        assert_eq!(
            value.measure(),
            u32::try_from(Report::worst_case_bytes(&CEILINGS).expect("wire bound")).expect("u32 ceiling")
        );
        let mut writer = Writer::new(usize::try_from(value.measure()).expect("u32 fits usize"));
        value.encode(&mut writer).expect("measured room");
        let wire = writer.finish();
        drop(value);

        let bound = Report::worst_case_heap(&CEILINGS).expect("heap bound");
        let meter = Meter::new();
        meter.start();
        let decoded = Report::decode(&CEILINGS, &mut Reader::new(&wire)).expect("full record");
        let measured = meter.end();
        assert_eq!(measured.peak(), bound, "the full value attains the heap bound");
        assert_eq!(measured.held(), bound, "the decoded value owns all of its heap");
        drop(decoded);
        assert_eq!(meter.held(), 0, "decoded allocations released");
    }
}
