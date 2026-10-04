//! The machine fed by hand: the handshake's first flight, records no TLS
//! server would write, the stream ending or failing before the handshake
//! and during it, closes, and the side above's bugs, asserted.

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Down, Fault, Read, Up};

use super::{LIMITS, Machine};
use crate::client::{self, Error, Event, Limits, Request, Waiting};

fn failed(error: Error) -> Vec<Event> {
    Vec::from([Event::Stream(Up::Failed(error.fault())), Event::Failed(error)])
}

/// A client that started its handshake: its `ClientHello` sent within the
/// room it demanded, with the first record's header, and that header
/// demanded again, alone.
fn started(limits: Limits) -> (Machine, Box<[u8]>) {
    let mut machine = Machine::new(limits);
    let (events, requests) = machine.down(Request::Handshake);
    assert_eq!(events, []);
    let [Down::Demand { read: Read::Fill(5), room }] = requests[..] else { panic!("{requests:?}") };
    assert!(room > 0 && room <= client::FLIGHT, "the hello's room: {room}");
    assert_eq!(machine.client.waiting(), Waiting::Handshaking);
    let (events, mut requests) = machine.up(Up::Room);
    assert_eq!(events, []);
    assert_eq!(requests.pop(), Some(Down::Demand { read: Read::Fill(client::HEADER), room: 0 }));
    let Some(Down::Send(hello)) = requests.pop() else { panic!("the hello sent") };
    assert_eq!(hello.len(), usize::try_from(room).unwrap(), "all the room asked for");
    (machine, hello)
}

#[test]
fn a_fresh_client_waits_for_the_handshake_and_closes_at_once() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.client.waiting(), Waiting::Handshake);
    assert_eq!(machine.down(Request::Close), (Vec::from([Event::Closed]), Vec::new()));
    assert_eq!(machine.client.waiting(), Waiting::Nothing);
}

#[test]
fn the_handshake_sends_a_client_hello_then_reads_a_record_at_a_time() {
    let (machine, hello) = started(LIMITS);
    // A handshake record (RFC 8446, 5.1) carrying a ClientHello (4).
    assert_eq!(hello[0], 22, "a handshake record");
    assert_eq!(hello[5], 1, "a ClientHello");
    let length = usize::from(u16::from_be_bytes([hello[3], hello[4]]));
    assert_eq!(hello.len(), 5 + length, "one record");
    assert_eq!(machine.client.waiting(), Waiting::Handshaking);
}

#[test]
fn the_stream_ending_or_failing_before_the_handshake_fails_it_when_asked() {
    for (ev, error) in [(Up::End, Error::Truncated), (Up::Failed(Fault::Reset), Error::Stream(Fault::Reset))] {
        let mut machine = Machine::new(LIMITS);
        assert_eq!(machine.up(ev), (Vec::new(), Vec::new()));
        // A failure after the end fails it all the same.
        assert_eq!(machine.up(Up::Failed(Fault::Other)), (Vec::new(), Vec::new()));
        let expected = if error == Error::Truncated { Error::Stream(Fault::Other) } else { error };
        assert_eq!(machine.down(Request::Handshake), (failed(expected), Vec::new()));
        assert_eq!(machine.client.waiting(), Waiting::Close);
        assert_eq!(machine.down(Request::Close), (Vec::from([Event::Closed]), Vec::new()));
    }
    let mut ended = Machine::new(LIMITS);
    assert_eq!(ended.up(Up::End), (Vec::new(), Vec::new()));
    assert_eq!(ended.down(Request::Handshake), (failed(Error::Truncated), Vec::new()));
}

#[test]
fn records_that_are_not_tls_fail_the_handshake_as_invalid() {
    for header in [
        &b"HTTP/"[..],               // no record type at all
        &[22, 3, 3, 0x48, 0x00][..], // a body past what TLS allows
        &[22, 3, 3, 0, 0][..],       // a handshake record of nothing
    ] {
        let (mut machine, _) = started(LIMITS);
        assert_eq!(machine.bytes(header), (failed(Error::Protocol), Vec::new()), "{header:?}");
        assert_eq!(machine.client.waiting(), Waiting::Close);
        // Nothing is outstanding below to withdraw.
        assert_eq!(machine.down(Request::Close), (Vec::from([Event::Closed]), Vec::new()));
    }
}

#[test]
fn a_fatal_alert_is_the_peer_giving_up() {
    let (mut machine, _) = started(LIMITS);
    let (events, requests) = machine.bytes(&[21, 3, 3, 0, 2]);
    assert_eq!(events, []);
    assert_eq!(requests, [Down::Demand { read: Read::Fill(2), room: 0 }], "the record's body");
    // A fatal handshake_failure (RFC 8446, 6.2).
    assert_eq!(machine.bytes(&[2, 40]), (failed(Error::Alert(40)), Vec::new()));
}

#[test]
fn a_handshake_message_longer_than_the_records_held_is_too_long() {
    let (mut machine, _) = started(LIMITS);
    // A ServerHello announced at 64 KB, its first 16 KB in one record.
    let (_, requests) = machine.bytes(&[22, 3, 3, 0x40, 0x00]);
    assert_eq!(requests, [Down::Demand { read: Read::Fill(16_384), room: 0 }]);
    let mut body = Vec::from([2, 0, 0xff, 0xff]);
    body.resize(16_384, 0);
    let (events, requests) = machine.bytes(&body);
    assert_eq!(events, []);
    assert_eq!(requests, [Down::Demand { read: Read::Fill(client::HEADER), room: 0 }], "the next record");
    // Its next record cannot be held beside the first.
    assert_eq!(machine.bytes(&[22, 3, 3, 0x40, 0x00]), (failed(Error::TooLong), Vec::new()));
}

#[test]
fn the_stream_ending_or_failing_during_the_handshake_fails_it() {
    let (mut ended, _) = started(LIMITS);
    assert_eq!(ended.up(Up::End), (failed(Error::Truncated), Vec::new()));
    let (mut broken, _) = started(LIMITS);
    assert_eq!(broken.up(Up::Failed(Fault::Other)), (failed(Error::Stream(Fault::Other)), Vec::new()));
    assert_eq!(broken.client.waiting(), Waiting::Close);
}

#[test]
fn a_close_during_the_handshake_withdraws_what_it_demanded_and_drops_what_comes_late() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.down(Request::Handshake));
    let withdrawal = Down::Demand { read: Read::Nothing, room: 0 };
    assert_eq!(machine.down(Request::Close), (Vec::from([Event::Closed]), Vec::from([withdrawal])));
    for late in [Up::Room, Up::Bytes(Box::from(&b"late"[..])), Up::End, Up::Failed(Fault::Reset)] {
        assert_eq!(machine.up(late), (Vec::new(), Vec::new()));
    }
    assert_eq!(machine.client.waiting(), Waiting::Nothing);
}

#[test]
fn a_demand_during_the_handshake_waits_for_it_and_a_failed_client_drops_what_follows() {
    let (mut machine, _) = started(LIMITS);
    let demand = Request::Stream(Down::Demand { read: Read::Fill(4), room: 8 });
    assert_eq!(machine.down(demand), (Vec::new(), Vec::new()), "held until Ready");
    assert_eq!(machine.client.waiting(), Waiting::Handshaking);
    drop(machine.up(Up::Failed(Fault::Reset)));
    // On their way when the failure went up.
    assert_eq!(machine.down(Request::Stream(Down::Finish)), (Vec::new(), Vec::new()));
    assert_eq!(machine.down(Request::Close), (Vec::from([Event::Closed]), Vec::new()));
}

#[test]
#[should_panic(expected = "the plaintext stream before the handshake was asked for")]
fn the_stream_before_the_handshake_is_a_bug() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 })));
}

#[test]
#[should_panic(expected = "one Handshake")]
fn a_second_handshake_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Handshake));
}

#[test]
#[should_panic(expected = "one demand at a time")]
fn a_second_demand_before_the_first_is_answered_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 })));
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 })));
}

#[test]
#[should_panic(expected = "no read past Limits::read")]
fn a_read_past_the_limit_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Fill(17), room: 0 })));
}

#[test]
#[should_panic(expected = "no room past Limits::send")]
fn room_past_the_limit_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Nothing, room: 17 })));
}

#[test]
#[should_panic(expected = "a Send within the room granted")]
fn a_send_without_room_granted_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Stream(Down::Send(Box::from(&b"x"[..])))));
}

#[test]
#[should_panic(expected = "Finish with no room demanded")]
fn finishing_with_room_demanded_is_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.down(Request::Stream(Down::Demand { read: Read::Nothing, room: 4 })));
    drop(machine.down(Request::Stream(Down::Finish)));
}

#[test]
#[should_panic(expected = "a Close after Close")]
fn a_close_after_close_is_a_bug() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.down(Request::Close));
    drop(machine.down(Request::Close));
}

#[test]
#[should_panic(expected = "an answer before the handshake demanded anything")]
fn an_answer_before_any_demand_is_a_bug() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.up(Up::Room));
}

#[test]
#[should_panic(expected = "bytes answer a fill, exactly")]
fn bytes_that_are_not_what_was_demanded_are_a_bug() {
    let (mut machine, _) = started(LIMITS);
    drop(machine.bytes(&[22, 3, 3, 0]));
}

#[test]
#[should_panic(expected = "the limits are honoured")]
fn limits_that_cannot_be_honoured_are_a_bug() {
    drop(Machine::new(Limits { records: 1, ..LIMITS }));
}
