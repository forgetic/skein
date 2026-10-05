//! SSE keeps its data stream name as an alias of lib's held buffer.

use skein_lib::stream::{Held, Read, Up};

use super::boxed;
use crate::sse::Data;

#[test]
fn data_is_a_held_stream() {
    let mut data: Held = Data::new(boxed(b"ok"));
    assert_eq!(data.answer(Read::Fill(2)), Some(Up::Bytes(boxed(b"ok"))));
    assert_eq!(data.answer(Read::Fill(1)), Some(Up::End));
}
