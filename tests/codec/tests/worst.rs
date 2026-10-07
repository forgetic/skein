//! Checked wire and heap worst cases (codec.md, section 4).

#[cfg(test)]
mod tests {
    use skein_codec_tests::{scalars, v1};
    use skein_lib::List;

    #[test]
    fn sample_wire_and_heap_ceilings_include_nested_lists() {
        assert_eq!(v1::Field::worst_case_bytes(&v1::CEILINGS), Some(16));
        assert_eq!(v1::Field::worst_case_heap(&v1::CEILINGS), Some(8));
        assert_eq!(v1::Report::worst_case_bytes(&v1::CEILINGS), Some(63));
        assert_eq!(v1::Choice::worst_case_bytes(&v1::CEILINGS), Some(64));
        assert_eq!(v1::worst_case_bytes(&v1::CEILINGS), Some(64));

        let list_heap = List::<v1::Field>::worst_case(2).expect("list heap fits");
        let report_heap = 24_u64.checked_add(list_heap).expect("sum fits");
        assert_eq!(v1::Report::worst_case_heap(&v1::CEILINGS), Some(report_heap));
        assert_eq!(v1::Choice::worst_case_heap(&v1::CEILINGS), Some(report_heap));
        assert_eq!(v1::worst_case_heap(&v1::CEILINGS), Some(report_heap));

        let mut invalid = v1::CEILINGS;
        invalid.report_fields = 3;
        assert_eq!(v1::worst_case_bytes(&invalid), None);
        assert_eq!(v1::worst_case_heap(&invalid), None);
    }

    #[test]
    fn scalar_family_has_no_heap_owned_by_decoded_values() {
        assert_eq!(scalars::Scalars::worst_case_bytes(&scalars::CEILINGS), Some(27));
        assert_eq!(scalars::worst_case_bytes(&scalars::CEILINGS), Some(27));
        assert_eq!(scalars::worst_case_heap(&scalars::CEILINGS), Some(0));
    }
}
