//! Scripted machine neighbour with separate read and whole-cap native ledgers.
use skein_channel::{
    Direction, Event, First, FrameWriter, KindRule, KnownKind, Limits, LowerEvent, LowerRequest, Machine, OpeningMode,
    OpeningProfile, Request, Role, Schema, Step, VersionRule,
};
use skein_lib::{Queue, Token, stream};

/// Independent literal direction/channel union includes V2-only and foreign kinds.
pub const KNOWN: [KnownKind; 7] = [
    KnownKind { kind: 257, channel: 1, direction: Direction::InitiatorToResponder },
    KnownKind { kind: 258, channel: 1, direction: Direction::InitiatorToResponder },
    KnownKind { kind: 385, channel: 1, direction: Direction::ResponderToInitiator },
    KnownKind { kind: 386, channel: 1, direction: Direction::ResponderToInitiator },
    KnownKind { kind: 641, channel: 2, direction: Direction::InitiatorToResponder },
    KnownKind { kind: 264, channel: 1, direction: Direction::InitiatorToResponder },
    KnownKind { kind: 392, channel: 1, direction: Direction::ResponderToInitiator },
];

/// Literal all-kind rows: foreign body 600 fixes the V1 unknown maximum.
pub const RULES: [KindRule; 12] = [
    KindRule { version: 1, kind: 258, body_bytes: 32 },
    KindRule { version: 1, kind: 257, body_bytes: 64 },
    KindRule { version: 1, kind: 385, body_bytes: 64 },
    KindRule { version: 1, kind: 386, body_bytes: 32 },
    KindRule { version: 1, kind: 641, body_bytes: 600 },
    KindRule { version: 2, kind: 258, body_bytes: 32 },
    KindRule { version: 2, kind: 257, body_bytes: 64 },
    KindRule { version: 2, kind: 385, body_bytes: 64 },
    KindRule { version: 2, kind: 386, body_bytes: 32 },
    KindRule { version: 2, kind: 641, body_bytes: 600 },
    KindRule { version: 2, kind: 264, body_bytes: 16 },
    KindRule { version: 2, kind: 392, body_bytes: 16 },
];

/// Tiny fixed source/storage caps; real native IO uses the same C and Q.
#[must_use]
pub fn limits() -> Limits {
    Limits {
        chunk_bytes: 7,
        queued_bytes: 1024,
        queued_frames: 4,
        schema_rows: 12,
        known_kinds: 7,
        versions: 2,
        terms: 32,
        refuse_bytes: 506,
    }
}

/// Checked concrete schema with every first gate installed before construction.
#[must_use]
pub fn schema(role: Role, mode: OpeningMode, first: bool, highest: u16) -> Schema {
    let gates = match role {
        Role::Initiator => First {
            receive: None,
            send: if first { Some(257) } else { None },
            initial_read: true,
            ping_before_receive: false,
        },
        Role::Responder => First {
            receive: if first { Some(257) } else { None },
            send: None,
            initial_read: true,
            ping_before_receive: first,
        },
    };
    let versions = [
        VersionRule { version: 1, unknown_body_bytes: 600, first: gates },
        VersionRule { version: 2, unknown_body_bytes: 600, first: gates },
    ];
    let rows = if highest == 1 { &RULES[..5] } else { &RULES[..] };
    Schema::new(
        role,
        OpeningProfile { magic: *b"tmpr", channel: 1, name_bytes: 64, secret_bytes: 64, mode, ping_allowed: true },
        limits(),
        &KNOWN,
        rows,
        &versions[..usize::from(highest)],
    )
    .expect("literal schema is within actual caps")
}

/// Independent frame spelling; never calls the production encoder as an oracle.
#[must_use]
pub fn literal(kind: u16, body: &[u8]) -> Box<[u8]> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&kind.to_be_bytes());
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend_from_slice(&u32::try_from(body.len()).expect("bounded test literal").to_be_bytes());
    bytes.extend_from_slice(body);
    bytes.into_boxed_slice()
}

/// Independent empty-credential Open body (17 bytes), with explicit offer.
#[must_use]
pub fn open_body(lowest: u16, highest: u16) -> Box<[u8]> {
    let mut bytes = Vec::from(&b"tmpr\x01"[..]);
    bytes.extend_from_slice(&lowest.to_be_bytes());
    bytes.extend_from_slice(&highest.to_be_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.into_boxed_slice()
}

/// Literal responder receive Terms preserve nonnumeric source order 258,257.
pub const RESPONDER_TERMS: &[u8] = &[0, 0, 0, 2, 1, 2, 0, 0, 0, 32, 1, 1, 0, 0, 0, 64];

/// Literal initiator receive Terms cover responder sends 385,386.
pub const INITIATOR_TERMS: &[u8] = &[0, 0, 0, 2, 1, 129, 0, 0, 0, 64, 1, 130, 0, 0, 0, 32];

/// Independent receiving ledger and captured actual frame movements.
pub struct Harness {
    /// Real shared machine, with no test-only alternate behavior.
    pub machine: Machine,
    /// Bounded upper scratch, inspected/dropped by the world.
    pub events: Queue<Event>,
    /// Bounded lower scratch, drained before each outward entrance.
    pub lower: Queue<LowerRequest>,
    /// Actual pending exact read, independently recorded from lower requests.
    pub read: Option<u32>,
    /// Actual pending native identity, independently recorded from Room.
    pub pending: Option<Token>,
    /// Actual retained native grant, consumed once by Send/Release.
    pub granted: Option<Token>,
    /// Native Sends as independent observed wire bytes.
    pub sent: Vec<Box<[u8]>>,
    /// Whether actual lower Finish was requested once.
    pub finished: bool,
}

impl Harness {
    /// Starts the real machine and its first explicit poll.
    #[must_use]
    pub fn new(schema: Schema) -> Harness {
        let mut harness = Harness {
            machine: Machine::new(schema),
            events: Queue::with_capacity(2),
            lower: Queue::with_capacity(3),
            read: None,
            pending: None,
            granted: None,
            sent: Vec::new(),
            finished: false,
        };
        harness.poll();
        harness
    }

    /// Checks each lower native/read ownership operation as the fake receives it.
    pub fn drain(&mut self) {
        while let Some(request) = self.lower.pop() {
            match request {
                LowerRequest::Stream(stream::Down::Demand { read, room }) => {
                    assert_eq!(room, 0, "native output is independent of read");
                    match read {
                        stream::Read::Fill(count) => {
                            assert!(self.read.replace(count).is_none(), "no overwrite of actual read");
                        }
                        stream::Read::Nothing => {
                            self.read = None;
                        }
                        stream::Read::Scan { .. } | stream::Read::Line { .. } => {
                            panic!("channel only demands exact fills")
                        }
                    }
                }
                LowerRequest::Stream(stream::Down::Finish) => {
                    assert!(!self.finished, "Finish exactly once");
                    assert!(self.read.is_none(), "Finish follows withdrawal");
                    self.finished = true;
                }
                LowerRequest::Stream(stream::Down::Send(_)) => panic!("native channel cannot emit classic Send"),
                LowerRequest::Output(stream::OutputDown::Room { right, bytes }) => {
                    assert_eq!(bytes, self.machine.schema().limits().queued_bytes);
                    assert!(self.pending.replace(right).is_none() && self.granted.is_none());
                }
                LowerRequest::Output(stream::OutputDown::Send { right, bytes }) => {
                    assert_eq!(self.granted.take(), Some(right), "one affine whole-cap grant");
                    assert!(
                        bytes.len()
                            <= usize::try_from(self.machine.schema().limits().queued_bytes).expect("C fits usize")
                    );
                    self.sent.push(bytes);
                }
                LowerRequest::Output(stream::OutputDown::Release { right }) => {
                    assert_eq!(self.granted.take(), Some(right), "release actual late/unused grant");
                }
                LowerRequest::Output(stream::OutputDown::Cancel { right }) => {
                    assert_eq!(self.pending, Some(right), "Cancel does not invent a terminal");
                }
            }
        }
    }

    /// Runs a separately authorized ordinary poll and drains its bounded effects.
    pub fn poll(&mut self) {
        skein_channel::poll(&mut self.machine, &mut self.events, &mut self.lower);
        self.drain();
    }

    /// A parent entrance followed by exactly its permitted final poll.
    pub fn request(&mut self, request: Request) -> Step {
        let step = skein_channel::down(&mut self.machine, request, &mut self.events, &mut self.lower);
        if step == Step::NeedPoll {
            skein_channel::poll(&mut self.machine, &mut self.events, &mut self.lower);
        }
        self.drain();
        step
    }

    /// Delivers one actual named winner, with independent affine grant ledger.
    pub fn grant(&mut self) {
        let right = self.pending.take().expect("actual pending native Room");
        assert!(self.granted.replace(right).is_none());
        let step = skein_channel::up(
            &mut self.machine,
            LowerEvent::Output(stream::OutputUp::Settled { right, outcome: stream::OutputOutcome::Granted }),
            &mut self.events,
            &mut self.lower,
        );
        if step == Step::NeedPoll {
            skein_channel::poll(&mut self.machine, &mut self.events, &mut self.lower);
        }
        self.drain();
    }

    /// Exact Bytes for the currently demanded read; leaves raw decode unresolved.
    pub fn bytes(&mut self, bytes: &[u8]) -> Step {
        let count = self.read.take().expect("actual read exists");
        assert_eq!(bytes.len(), usize::try_from(count).expect("read fits usize"));
        let step = skein_channel::up(
            &mut self.machine,
            LowerEvent::Stream(stream::Up::Bytes(Box::from(bytes))),
            &mut self.events,
            &mut self.lower,
        );
        if step == Step::NeedPoll {
            skein_channel::poll(&mut self.machine, &mut self.events, &mut self.lower);
        }
        self.drain();
        step
    }

    /// Independent peer frame split only at each actual demand; one final body.
    pub fn feed(&mut self, frame: &[u8]) -> Step {
        let mut offset = 0;
        let mut step = Step::Halt;
        while offset < frame.len() {
            let length = usize::try_from(self.read.expect("framed peer has exact demand")).expect("count fits usize");
            let end = offset.checked_add(length).expect("bounded frame");
            step = self.bytes(&frame[offset..end]);
            offset = end;
        }
        step
    }

    /// Independent literal opening/Terms exchange for a ready responder.
    pub fn ready_responder(first: bool) -> Harness {
        let mut harness = Harness::new(schema(Role::Responder, OpeningMode::AcceptHighest, first, 1));
        harness.grant();
        assert_eq!(harness.feed(&literal(1, &open_body(0, 1))), Step::NeedPoll);
        // Accept moved once; local Terms await the next actual whole-cap grant.
        harness.grant();
        harness.feed(&literal(16, INITIATOR_TERMS));
        match harness.events.pop().expect("Ready emitted") {
            Event::Ready { version: 1 } => {}
            event => panic!("unexpected {event:?}"),
        }
        harness
    }

    /// Producer measures, writes directly, then submits the sealed real frame.
    pub fn send(&mut self, kind: u16, body: &[u8], owner: u64) -> Step {
        let mut writer = FrameWriter::new(
            self.machine.schema(),
            self.machine.version(),
            kind,
            u32::try_from(body.len()).expect("bounded body"),
        )
        .expect("typed test body matches sender schema");
        writer.put(body).expect("measured exact body");
        let encoded = writer.finish().expect("exact final frame");
        self.request(Request::Send { owner: Token::new(owner), encoded: Some(encoded) })
    }
}
