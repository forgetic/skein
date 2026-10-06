//! Deterministic sweeps exercise real machine entrances; malformed reference
//! header checks are independent fixed-array byte arithmetic.
use crate::machine::{Harness, literal, open_body, schema};
use skein_channel::{Event, LowerEvent, OpeningMode, Request, Role, Step};
use skein_lib::{Rng, stream};

/// Independent header validity oracle (exact eight, reserved zero).
#[must_use]
pub fn header_oracle(bytes: &[u8]) -> Option<(u16, u32)> {
    if bytes.len() != 8 || bytes[2] != 0 || bytes[3] != 0 {
        return None;
    }
    Some((u16::from_be_bytes([bytes[0], bytes[1]]), u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])))
}

/// Bounded random raw header/closing sequences with exact independent oracle.
pub fn raw(seed: u64, cases: u32) {
    let mut rng = Rng::new(seed);
    for _ in 0..cases {
        let mut bytes = [0_u8; 8];
        for byte in &mut bytes {
            *byte = u8::try_from(rng.below(256)).expect("bounded random byte");
        }
        assert_eq!(
            skein_channel::framing(&bytes).map(|header| (header.kind, header.body_bytes)),
            header_oracle(&bytes)
        );
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 1));
        server.bytes(&bytes);
        while server.events.pop().is_some() {}
        server.request(Request::Close);
        while server.events.pop().is_some() {}
        if server.pending.is_some() {
            server.grant();
        }
        skein_channel::up(&mut server.machine, LowerEvent::Closed, &mut server.events, &mut server.lower);
        assert!(server.machine.is_retired());
        assert!(server.machine.pending_bytes() == 0);
    }
}

/// Peer offers include zero/outside versions; independent intersection oracle.
pub fn versions(seed: u64, cases: u32) {
    let mut rng = Rng::new(seed);
    for _ in 0..cases {
        let lowest = u16::try_from(rng.below(5)).expect("small version");
        let highest = u16::try_from(rng.below(5)).expect("small version");
        let mut server = Harness::new(schema(Role::Responder, OpeningMode::AskParent, false, 2));
        server.feed(&literal(1, &open_body(lowest, highest)));
        let event = server.events.pop().expect("opening or closure");
        if lowest <= highest && lowest <= 2 && highest >= 1 {
            assert!(matches!(event, Event::Opening { .. }));
        } else {
            let expected = if lowest > highest { skein_channel::Fault::Framing } else { skein_channel::Fault::Version };
            assert!(matches!(event,Event::Closed {fault} if fault==expected));
        }
        server.request(Request::Close);
        while server.events.pop().is_some() {}
        server.grant();
        let step = skein_channel::up(
            &mut server.machine,
            LowerEvent::Stream(stream::Up::End),
            &mut server.events,
            &mut server.lower,
        );
        assert_eq!(step, Step::Halt);
        skein_channel::up(&mut server.machine, LowerEvent::Closed, &mut server.events, &mut server.lower);
        assert!(server.machine.is_retired());
    }
}
