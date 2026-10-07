//! Deterministic arbitrary wire bytes for every top-level sample record
//! (codec.md, section 6; testing-strategy.md, section 8).

#[cfg(test)]
mod tests {
    use skein_codec_tests::{scalars, v1};
    use skein_lib::{Reader, Rng, Writer};

    fn check_field(bytes: &[u8]) {
        if let Ok(value) = v1::Field::decode(&v1::CEILINGS, &mut Reader::new(bytes)) {
            let mut writer = Writer::new(usize::try_from(value.measure()).expect("measured length"));
            value.encode(&mut writer).expect("measured room");
            assert_eq!(writer.finish().as_ref(), bytes);
        }
    }

    fn check_report(bytes: &[u8]) {
        if let Ok(value) = v1::Report::decode(&v1::CEILINGS, &mut Reader::new(bytes)) {
            let mut writer = Writer::new(usize::try_from(value.measure()).expect("measured length"));
            value.encode(&mut writer).expect("measured room");
            assert_eq!(writer.finish().as_ref(), bytes);
        }
    }

    fn check_marker(bytes: &[u8]) {
        if let Ok(value) = v1::Marker::decode(&v1::CEILINGS, &mut Reader::new(bytes)) {
            let mut writer = Writer::new(usize::try_from(value.measure()).expect("measured length"));
            value.encode(&mut writer).expect("measured room");
            assert_eq!(writer.finish().as_ref(), bytes);
        }
    }

    fn check_empty(bytes: &[u8]) {
        if let Ok(value) = scalars::Empty::decode(&scalars::CEILINGS, &mut Reader::new(bytes)) {
            let mut writer = Writer::new(usize::try_from(value.measure()).expect("measured length"));
            value.encode(&mut writer).expect("measured room");
            assert_eq!(writer.finish().as_ref(), bytes);
        }
    }

    fn check_scalars(bytes: &[u8]) {
        if let Ok(value) = scalars::Scalars::decode(&scalars::CEILINGS, &mut Reader::new(bytes)) {
            let mut writer = Writer::new(usize::try_from(value.measure()).expect("measured length"));
            value.encode(&mut writer).expect("measured room");
            assert_eq!(writer.finish().as_ref(), bytes);
        }
    }

    fn check_all(bytes: &[u8]) {
        check_field(bytes);
        check_report(bytes);
        check_marker(bytes);
        check_empty(bytes);
        check_scalars(bytes);
    }

    #[test]
    fn arbitrary_and_mutated_golden_bytes_never_panic_and_round_trip_if_valid() {
        let mut rng = Rng::new(0xC0DE_CAFE_8100_0001);
        let mut buffer = [0_u8; 128];
        for _round in 0_u32..10_000 {
            let length = usize::try_from(rng.below(129)).expect("bounded length");
            for byte in buffer.get_mut(..length).expect("within buffer") {
                *byte = u8::try_from(rng.next_u64() & 0xff).expect("low byte");
            }
            check_all(buffer.get(..length).expect("within buffer"));
        }

        for name in [
            "record_field_full.bin",
            "record_report_full.bin",
            "record_marker_full.bin",
            "record_empty_full.bin",
            "record_scalars_full.bin",
        ] {
            let golden = std::fs::read(std::path::Path::new("golden/v1").join(name)).expect("golden readable");
            check_all(&golden);
            for _round in 0_u32..200 {
                let mut mutated = golden.clone();
                if !mutated.is_empty() {
                    let index = usize::try_from(rng.below(u64::try_from(mutated.len()).expect("length fits")))
                        .expect("index fits");
                    *mutated.get_mut(index).expect("chosen byte") =
                        u8::try_from(rng.next_u64() & 0xff).expect("low byte");
                }
                check_all(&mutated);
            }
        }
    }
}
