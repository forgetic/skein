//! A held buffer as a stream (lib.md, 7): each demand answered at once,
//! by exactly what it reads, or by the end.

use alloc::boxed::Box;

use crate::stream::{Delimiter, Held, Read, Up};

fn boxed(bytes: &[u8]) -> Box<[u8]> {
    Box::from(bytes)
}

#[test]
fn fills_and_scans_are_met_from_the_buffer_then_the_end_comes_once() {
    let mut data = Held::new(boxed(b"{\"a\":\"bc\"}"));
    assert_eq!(data.answer(Read::Fill(1)), Some(Up::Bytes(boxed(b"{"))));
    let quote = Delimiter::new(b"\"").expect("one byte");
    assert_eq!(data.answer(Read::Scan { until: quote, max: 4 }), Some(Up::Bytes(boxed(b"\""))));
    assert_eq!(data.answer(Read::Scan { until: quote, max: 4 }), Some(Up::Bytes(boxed(b"a\""))));
    assert_eq!(data.answer(Read::Scan { until: quote, max: 2 }), Some(Up::Bytes(boxed(b":\""))));
    assert_eq!(data.answer(Read::Scan { until: quote, max: 1 }), Some(Up::Bytes(boxed(b"b"))), "its maximum");
    assert_eq!(data.answer(Read::Fill(3)), Some(Up::Bytes(boxed(b"c\"}"))));
    assert_eq!(data.answer(Read::Fill(0)), Some(Up::Bytes(boxed(b""))));
    assert_eq!(data.answer(Read::Fill(1)), Some(Up::End));
    assert_eq!(data.answer(Read::Fill(1)), None, "nothing after the end");
}

#[test]
fn a_read_larger_than_what_is_left_is_the_end() {
    let mut data = Held::new(boxed(b"abc"));
    assert_eq!(data.answer(Read::Fill(4)), Some(Up::End));
    let mut data = Held::new(boxed(b"abc"));
    assert_eq!(
        data.answer(Read::Scan { until: Delimiter::LF, max: 4 }),
        Some(Up::End),
        "no LF, and short of the maximum"
    );
    let mut data = Held::new(boxed(b"abc"));
    assert_eq!(data.answer(Read::Scan { until: Delimiter::LF, max: 3 }), Some(Up::Bytes(boxed(b"abc"))));
}

#[test]
fn a_withdrawal_is_not_answered() {
    let mut data = Held::new(boxed(b"abc"));
    assert_eq!(data.answer(Read::Nothing), None);
    assert_eq!(data.answer(Read::Fill(3)), Some(Up::Bytes(boxed(b"abc"))));
}

#[test]
fn a_delimiter_must_end_within_the_maximum() {
    let mut data = Held::new(boxed(b"ab\r\ncd"));
    assert_eq!(data.answer(Read::Scan { until: Delimiter::CRLF, max: 3 }), Some(Up::Bytes(boxed(b"ab\r"))));
    assert_eq!(data.answer(Read::Scan { until: Delimiter::CRLF, max: 3 }), Some(Up::Bytes(boxed(b"\ncd"))));
}
