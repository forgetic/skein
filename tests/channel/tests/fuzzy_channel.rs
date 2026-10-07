//! Seeded header, opening, terms and channel worlds (channel.md, section 11).
use skein_channel::{
    Direction, Event, Kind, Limits, Lower, LowerEvent, Machine, Role, Schema, Version, decode_control, parse_header,
};
use skein_lib::{List, Queue, Rng, stream};

#[path = "generic_protocol.rs"]
mod protocol;

fn schema() -> Schema {
    let mut kinds = List::with_capacity(1);
    kinds.push(Kind { kind: 0x0100, direction: Direction::FromInitiator, largest: 64 }).expect("kind");
    let mut versions = List::with_capacity(1);
    versions.push(Version { version: 1, kinds }).expect("version");
    Schema { magic: *b"fuzz", versions }
}

fn limits() -> Limits {
    Limits { chunk: 5, credential: 16, skip: 32, output_bytes: 128, output_frames: 2, kinds: 1 }
}

#[test]
fn fuzzy_header_opening_and_terms_on_a_machine() {
    for seed in 0..512_u64 {
        let mut rng = Rng::new(seed);
        let mut header = [0_u8; 8];
        for byte in &mut header {
            *byte = u8::try_from(rng.below(256)).expect("byte");
        }
        let _ = parse_header(&header);
        let mut body = [0_u8; 32];
        for byte in &mut body {
            *byte = u8::try_from(rng.below(256)).expect("byte");
        }
        for len in 0..body.len() {
            let _ = decode_control(1, &body[..len], &limits());
            let _ = decode_control(4, &body[..len], &limits());
        }

        let mut machine = Machine::new(schema(), Role::Responder, limits()).expect("schema");
        let mut above = Queue::<Event>::with_capacity(8);
        let mut below = Queue::<Lower>::with_capacity(8);
        machine.poll(&mut above, &mut below);
        let mut saw_demand = false;
        while let Some(record) = below.pop() {
            if let Lower::Read(stream::Down::Demand { read: stream::Read::Fill(1), room: 0 }) = record {
                saw_demand = true;
            }
        }
        assert!(saw_demand, "seed {seed}");
        machine.up(LowerEvent::Read(stream::Up::Bytes(Box::from([header[0]]))), &mut above, &mut below);
        machine.poll(&mut above, &mut below);
        let mut saw_rest = false;
        while let Some(record) = below.pop() {
            if let Lower::Read(stream::Down::Demand { read: stream::Read::Fill(7), room: 0 }) = record {
                saw_rest = true;
            }
        }
        assert!(saw_rest, "seed {seed}");
        machine.up(LowerEvent::Read(stream::Up::Bytes(Box::from(&header[1..]))), &mut above, &mut below);
        machine.poll(&mut above, &mut below);
    }
}

#[test]
fn fuzzy_two_channel_protocol_worlds() {
    for seed in 0..64_u64 {
        protocol::seeded_protocol(seed);
    }
}
