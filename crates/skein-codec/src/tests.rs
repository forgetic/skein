use super::{Reason, read_count, read_len, read_text, version_of};
use skein_lib::Reader;

#[test]
fn length_checks_header_limit_and_remaining() {
    assert_eq!(read_len(&mut Reader::new(&[0, 0, 0]), 2), Err(Reason::Short));
    assert_eq!(read_len(&mut Reader::new(&[0, 0, 0, 3, 1, 2, 3]), 2), Err(Reason::Bound));
    assert_eq!(read_len(&mut Reader::new(&[0, 0, 0, 2, 1]), 2), Err(Reason::Short));
    assert_eq!(read_len(&mut Reader::new(&[0, 0, 0, 2, 1, 2]), 2), Ok(2));
    assert_eq!(read_len(&mut Reader::new(&[0, 0, 0, 0]), 0), Ok(0));
}

#[test]
fn count_checks_header_and_limit() {
    assert_eq!(read_count(&mut Reader::new(&[0, 0, 0]), 1), Err(Reason::Short));
    assert_eq!(read_count(&mut Reader::new(&[0, 0, 0, 2]), 1), Err(Reason::Bound));
    assert_eq!(read_count(&mut Reader::new(&[0, 0, 0, 2]), 2), Ok(2));
    assert_eq!(read_count(&mut Reader::new(&[0, 0, 0, 0]), 0), Ok(0));
}

#[test]
fn text_checks_bytes_and_utf8() {
    assert_eq!(read_text(&mut Reader::new(&[0, 0, 0, 2, 0xff, 0xff]), 2), Err(Reason::Utf8));
    assert_eq!(read_text(&mut Reader::new(&[0, 0, 0, 2, b'a']), 2), Err(Reason::Short));
    assert_eq!(read_text(&mut Reader::new(&[0, 0, 0, 2, b'a', b'b']), 1), Err(Reason::Bound));
    assert_eq!(read_text(&mut Reader::new(&[0, 0, 0, 2, b'a', b'b']), 2).as_deref(), Ok(&b"ab"[..]));
    assert_eq!(read_text(&mut Reader::new(&[0, 0, 0, 0]), 0).as_deref(), Ok(&b""[..]));
}

#[test]
fn version_reads_only_prefix() {
    assert_eq!(version_of(&[0]), Err(Reason::Short));
    assert_eq!(version_of(&[0, 3, 99]), Ok(3));
}

#[test]
fn every_reason_is_distinct() {
    let reasons =
        [Reason::Short, Reason::Bound, Reason::Tag, Reason::Bool, Reason::Utf8, Reason::Version, Reason::Trailing];
    for (index, first) in reasons.iter().enumerate() {
        for (other_index, second) in reasons.iter().enumerate() {
            if index != other_index {
                assert_ne!(first, second);
            }
        }
    }
}
