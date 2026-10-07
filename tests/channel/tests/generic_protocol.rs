//! Two generic channels joined over socket-like and separate-pipe in-memory
//! streams (channel.md, sections 7 and 11; testing-strategy.md, section 2.5).
use std::collections::VecDeque;

use skein_channel::{
    Direction, Event, Kind, Limits, Lower, LowerEvent, Machine, Request, Role, Schema, StreamMode, Version,
    frame_writer,
};
use skein_lib::{List, Queue, Rng, Token, stream};

#[derive(Clone, Copy)]
enum Mode {
    Socket,
    Pipes,
}

struct Wire {
    mode: Mode,
    initiator_to_responder: VecDeque<u8>,
    responder_to_initiator: VecDeque<u8>,
}

impl Wire {
    fn new(mode: Mode) -> Wire {
        Wire { mode, initiator_to_responder: VecDeque::new(), responder_to_initiator: VecDeque::new() }
    }

    fn outgoing(&mut self, role: Role) -> &mut VecDeque<u8> {
        match self.mode {
            Mode::Socket | Mode::Pipes => match role {
                Role::Initiator => &mut self.initiator_to_responder,
                Role::Responder => &mut self.responder_to_initiator,
            },
        }
    }

    fn incoming(&mut self, role: Role) -> &mut VecDeque<u8> {
        match self.mode {
            Mode::Socket | Mode::Pipes => match role {
                Role::Initiator => &mut self.responder_to_initiator,
                Role::Responder => &mut self.initiator_to_responder,
            },
        }
    }
}

struct Peer {
    role: Role,
    machine: Machine,
    above: Queue<Event>,
    below: Queue<Lower>,
    pending: Option<usize>,
    ready: bool,
    received: Option<(u16, Box<[u8]>)>,
    drained: u32,
    output_failed: bool,
}

impl Peer {
    fn new(role: Role, schema: Schema, limits: Limits, mode: Mode) -> Peer {
        let stream_mode = match mode {
            Mode::Socket => StreamMode::One,
            Mode::Pipes => StreamMode::Two,
        };
        Peer {
            role,
            machine: Machine::with_mode(schema, role, limits, stream_mode).expect("checked schema"),
            above: Queue::with_capacity(16),
            below: Queue::with_capacity(16),
            pending: None,
            ready: false,
            received: None,
            drained: 0,
            output_failed: false,
        }
    }

    fn down(&mut self, request: Request) {
        self.machine.down(request, &mut self.above, &mut self.below);
    }

    #[expect(clippy::wildcard_enum_match_arm, reason = "the world fails on unexpected channel events")]
    fn step(&mut self, wire: &mut Wire) {
        self.machine.poll(&mut self.above, &mut self.below);
        while let Some(record) = self.below.pop() {
            match record {
                Lower::Read(stream::Down::Demand { read: stream::Read::Fill(count), room: 0 }) => {
                    self.pending = Some(usize::try_from(count).expect("bounded read"));
                }
                Lower::Write(stream::OutputDown::Room { right, .. }) => {
                    self.machine.up(
                        LowerEvent::Write(stream::OutputUp::Settled { right, outcome: stream::OutputOutcome::Granted }),
                        &mut self.above,
                        &mut self.below,
                    );
                }
                Lower::Write(stream::OutputDown::Send { bytes, .. }) => {
                    wire.outgoing(self.role).extend(bytes.iter().copied());
                }
                Lower::FinishWrite => {}
                record => panic!("unexpected lower record: {record:?}"),
            }
        }
        if let Some(count) = self.pending
            && wire.incoming(self.role).len() >= count
        {
            let mut bytes = Vec::with_capacity(count);
            for _ in 0..count {
                bytes.push(wire.incoming(self.role).pop_front().expect("enough bytes"));
            }
            self.pending = None;
            self.machine.up(
                LowerEvent::Read(stream::Up::Bytes(bytes.into_boxed_slice())),
                &mut self.above,
                &mut self.below,
            );
        }
        while let Some(event) = self.above.pop() {
            match event {
                Event::Opening { .. } => self.down(Request::Accept { version: 1 }),
                Event::Ready { version: 1, .. } => self.ready = true,
                Event::Drained => self.drained = self.drained.checked_add(1).expect("bounded count"),
                Event::OutputFailed => self.output_failed = true,
                Event::Sent { .. } => {}
                Event::Body { kind, body } => self.received = Some((kind, body)),
                event => panic!("unexpected owner event: {event:?}"),
            }
        }
    }
}

/// Runs both channel machines over deterministic byte cuts for the fuzzy suite.
pub fn seeded_protocol(seed: u64) {
    let mut rng = Rng::new(seed);
    for mode in [Mode::Socket, Mode::Pipes] {
        let mut wire = Wire::new(mode);
        let mut seeded_limits = limits();
        seeded_limits.chunk = u32::try_from(rng.below(7) + 1).expect("small chunk");
        let mut initiator = Peer::new(Role::Initiator, schema(), seeded_limits, mode);
        let mut responder = Peer::new(Role::Responder, schema(), seeded_limits, mode);
        initiator.down(Request::Open { credential: Box::from([]) });
        for _ in 0..500 {
            initiator.step(&mut wire);
            responder.step(&mut wire);
            if initiator.ready && responder.ready {
                break;
            }
        }
        assert!(initiator.ready && responder.ready, "seed {seed}");
        let left_len = usize::try_from(rng.below(32) + 1).expect("bounded length");
        let right_len = usize::try_from(rng.below(32) + 1).expect("bounded length");
        let mut left = frame_writer(0x0100, u32::try_from(left_len).expect("length")).expect("frame");
        left.put(&vec![17; left_len]).expect("body");
        let mut right = frame_writer(0x0101, u32::try_from(right_len).expect("length")).expect("frame");
        right.put(&vec![29; right_len]).expect("body");
        initiator.down(Request::Read);
        responder.down(Request::Read);
        initiator.down(Request::Send { token: Token::new(1), frame: left.finish().expect("frame") });
        responder.down(Request::Send { token: Token::new(2), frame: right.finish().expect("frame") });
        for _ in 0..200 {
            initiator.step(&mut wire);
            responder.step(&mut wire);
            if initiator.received.is_some() && responder.received.is_some() {
                break;
            }
        }
        assert_eq!(initiator.received.expect("response").1.as_ref(), vec![29; right_len], "seed {seed}");
        assert_eq!(responder.received.expect("request").1.as_ref(), vec![17; left_len], "seed {seed}");
    }
}

fn schema() -> Schema {
    let mut kinds = List::with_capacity(2);
    kinds.push(Kind { kind: 0x0100, direction: Direction::FromInitiator, largest: 32 }).expect("first kind");
    kinds.push(Kind { kind: 0x0101, direction: Direction::FromResponder, largest: 32 }).expect("second kind");
    let mut versions = List::with_capacity(1);
    versions.push(Version { version: 1, kinds }).expect("one version");
    Schema { magic: *b"dupx", versions }
}

fn limits() -> Limits {
    Limits { chunk: 7, credential: 8, skip: 64, output_bytes: 128, output_frames: 2, kinds: 2 }
}

#[test]
fn both_directions_progress_over_socket_and_pipes() {
    for mode in [Mode::Socket, Mode::Pipes] {
        let mut wire = Wire::new(mode);
        let mut initiator = Peer::new(Role::Initiator, schema(), limits(), mode);
        let mut responder = Peer::new(Role::Responder, schema(), limits(), mode);
        initiator.down(Request::Open { credential: Box::from([]) });
        for _ in 0_u32..80_u32 {
            initiator.step(&mut wire);
            responder.step(&mut wire);
            if initiator.ready && responder.ready {
                break;
            }
        }
        assert!(initiator.ready && responder.ready);
        let mut from_initiator = frame_writer(0x0100, 17).expect("measured frame");
        from_initiator.put(&[1_u8; 17]).expect("body fits");
        let mut from_responder = frame_writer(0x0101, 19).expect("measured frame");
        from_responder.put(&[2_u8; 19]).expect("body fits");
        initiator.down(Request::Read);
        responder.down(Request::Read);
        initiator.down(Request::Send { token: Token::new(11), frame: from_initiator.finish().expect("frame") });
        responder.down(Request::Send { token: Token::new(22), frame: from_responder.finish().expect("frame") });
        for _ in 0_u32..60_u32 {
            initiator.step(&mut wire);
            responder.step(&mut wire);
            if initiator.received.is_some() && responder.received.is_some() {
                break;
            }
        }
        let (kind, bytes) = initiator.received.expect("initiator got responder frame");
        assert_eq!(kind, 0x0101);
        assert_eq!(&*bytes, &[2_u8; 19]);
        let (kind, bytes) = responder.received.expect("responder got initiator frame");
        assert_eq!(kind, 0x0100);
        assert_eq!(&*bytes, &[1_u8; 17]);
        assert_eq!(initiator.drained, 3);
        assert_eq!(responder.drained, 3);
    }
}

#[test]
fn a_last_word_reaches_the_peer_after_the_writer_pipe_fails() {
    let mut wire = Wire::new(Mode::Pipes);
    let mut reader = Peer::new(Role::Initiator, schema(), limits(), Mode::Pipes);
    let mut writer = Peer::new(Role::Responder, schema(), limits(), Mode::Pipes);
    reader.down(Request::Open { credential: Box::from([]) });
    for _ in 0_u32..80_u32 {
        reader.step(&mut wire);
        writer.step(&mut wire);
        if reader.ready && writer.ready {
            break;
        }
    }
    assert!(reader.ready && writer.ready);
    reader.down(Request::Read);
    let mut frame = frame_writer(0x0101, 9).expect("measured last word");
    frame.put(b"last word").expect("body fits");
    writer.down(Request::Send { token: Token::new(91), frame: frame.finish().expect("frame") });
    for _ in 0_u32..5_u32 {
        writer.step(&mut wire);
        if writer.drained == 3 {
            break;
        }
    }
    assert_eq!(writer.drained, 3);
    writer.machine.up(LowerEvent::WriteFailed(stream::Fault::Reset), &mut writer.above, &mut writer.below);
    writer.step(&mut wire);
    assert!(writer.output_failed);
    for _ in 0_u32..20_u32 {
        reader.step(&mut wire);
        if reader.received.is_some() {
            break;
        }
    }
    let (kind, body) = reader.received.expect("last word reached the peer");
    assert_eq!(kind, 0x0101);
    assert_eq!(&*body, b"last word");
}
