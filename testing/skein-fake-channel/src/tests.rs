use skein_channel::{Direction, Kind, Limits, Role, Schema, Version, frame_writer};
use skein_lib::List;

use super::ScriptedPeer;

fn pump(source: &mut ScriptedPeer, destination: &mut ScriptedPeer) {
    while let Some(frame) = source.pop_output() {
        for cut in frame.chunks(3) {
            destination.feed(cut).expect("opening cut");
        }
    }
}

fn schema() -> Schema {
    let mut kinds = List::with_capacity(2);
    kinds.push(Kind { kind: 0x0100, direction: Direction::FromInitiator, largest: 12 }).expect("room");
    kinds.push(Kind { kind: 0x0101, direction: Direction::FromResponder, largest: 12 }).expect("room");
    let mut versions = List::with_capacity(1);
    versions.push(Version { version: 3, kinds }).expect("room");
    Schema { magic: *b"fake", versions }
}

fn limits() -> Limits {
    Limits { chunk: 3, credential: 16, skip: 16, output_bytes: 64, output_frames: 2, kinds: 2 }
}

#[test]
fn peers_open_in_arbitrary_cuts_then_play_and_check_frames() {
    let mut initiator =
        ScriptedPeer::new(schema(), Role::Initiator, limits(), 3, Box::from(b"secret".as_slice())).expect("initiator");
    let mut responder = ScriptedPeer::new(schema(), Role::Responder, limits(), 3, Box::from([])).expect("responder");

    for _ in 0..4 {
        pump(&mut initiator, &mut responder);
        pump(&mut responder, &mut initiator);
    }
    assert!(initiator.is_ready() && responder.is_ready());
    assert_eq!(responder.credential(), b"secret");

    let mut sent = frame_writer(0x0100, 5).expect("writer");
    sent.put(b"hello").expect("body");
    initiator.play(sent.finish().expect("frame"));
    responder.expect(0x0100, Box::from(b"hello".as_slice()));
    let frame = initiator.pop_output().expect("scripted frame");
    for cut in frame.chunks(2) {
        responder.feed(cut).expect("scripted cut");
    }
    assert_eq!(responder.observed().len(), 1);
    initiator.verify().expect("initiator finished");
    responder.verify().expect("responder finished");
}
