//! The intake against a plain reference (lib.md, 7), over many random
//! inputs, demands, caps and splits.

use skein_lib::stream::{Delimiter, Read};
use skein_lib::{Intake, Rng};
use skein_lib_tests::intake::{check, drain, reference, scan};

#[test]
fn random_splits_demands_and_caps_meet_what_the_reference_meets() {
    let delimiters = [Delimiter::LF, Delimiter::CRLF, Delimiter::CRLF_CRLF, Delimiter::new(b"a\r").unwrap()];
    let mut rng = Rng::new(0x1_47A4E);
    for _ in 0_u32..20_000 {
        let capacity = u32::try_from(rng.between(4, 24)).unwrap();
        let mut input = Vec::new();
        for _ in 0..rng.below(96) {
            input.push(*[b'a', b'b', b'\r', b'\n'].get(usize::try_from(rng.below(4)).unwrap()).unwrap());
        }
        let mut demands = Vec::new();
        for _ in 0..rng.between(1, 40) {
            let read = match rng.below(6) {
                0 => Read::Nothing,
                1 => Read::Fill(u32::try_from(rng.below(u64::from(capacity) + 1)).unwrap()),
                2 => Read::Line { max: u32::try_from(rng.between(1, u64::from(capacity))).unwrap() },
                _ => {
                    let until = delimiters[usize::try_from(rng.below(4)).unwrap()];
                    let shortest = u64::try_from(until.as_bytes().len()).unwrap();
                    scan(until, u32::try_from(rng.between(shortest, u64::from(capacity))).unwrap())
                }
            };
            demands.push(read);
        }
        let mut pieces = Vec::new();
        for _ in 0_u32..8 {
            pieces.push(usize::try_from(rng.between(1, 12)).unwrap());
        }
        check(&input, &demands, capacity, &pieces);
    }
}

#[test]
fn every_meet_takes_what_the_buffer_holds_demands_changing_or_not() {
    // Few letters and overlapping delimiters make many partial matches;
    // a demand left unmet is often followed by a different one.
    let delimiters: [&[u8]; 11] =
        [b"a", b"aa", b"aaa", b"aaaa", b"ab", b"aba", b"abab", b"aab", b"aaab", b"abaa", b"bab"];
    let mut rng = Rng::new(0x3E_E7);
    for round in 0_u32..20_000 {
        let capacity = u32::try_from(rng.between(0, 12)).unwrap();
        let cap = usize::try_from(capacity).unwrap();
        let mut intake = Intake::with_capacity(capacity);
        let mut model = Vec::new();
        for op in 0_u32..60 {
            if rng.chance(500) {
                let mut bytes = [0_u8; 6];
                let len = usize::try_from(rng.below(7)).unwrap();
                for byte in &mut bytes[..len] {
                    *byte = if rng.chance(500) { b'a' } else { b'b' };
                }
                let fits = model.len().checked_add(len).unwrap() <= cap;
                let appended = intake.append(&bytes[..len]);
                assert_eq!(appended.is_ok(), fits, "round {round}, op {op}");
                if fits {
                    model.extend_from_slice(&bytes[..len]);
                }
            } else {
                let read = match rng.below(4) {
                    0 => Read::Nothing,
                    1 => Read::Fill(u32::try_from(rng.below(u64::from(capacity) + 1)).unwrap()),
                    _ => {
                        let until = delimiters[usize::try_from(rng.below(11)).unwrap()];
                        let shortest = u32::try_from(until.len()).unwrap();
                        if shortest > capacity {
                            continue;
                        }
                        let max = u32::try_from(rng.between(u64::from(shortest), u64::from(capacity))).unwrap();
                        scan(Delimiter::new(until).unwrap(), max)
                    }
                };
                let expected = reference(&model, read);
                let delivered = intake.meet(read);
                match expected {
                    Some(n) => {
                        assert_eq!(delivered.as_deref(), Some(&model[..n]), "round {round}, op {op}: {read:?}");
                        drain(&mut model, n);
                    }
                    None => assert_eq!(delivered, None, "round {round}, op {op}: {read:?}"),
                }
            }
            let len = u32::try_from(model.len()).unwrap();
            assert_eq!(
                (intake.len(), intake.room()),
                (len, capacity.checked_sub(len).unwrap()),
                "round {round}, op {op}"
            );
            assert_eq!(intake.is_empty(), model.is_empty());
        }
    }
}
