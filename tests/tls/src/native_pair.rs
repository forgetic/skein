//! Lowest-tier native TLS peer harness, independent read/output ledgers beside
//! real rustls records. Actual IO capacity/lifetime proof is native.rs, not this
//! prompt bounded proxy (tls.md, 3.6; testing-strategy.md, 2.1 and 2.4).
use crate::{native_peer::Tls12, pki, server::Server};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Env, Queue, Time, Token};
use skein_tls::client::{self, Limits, native};
use skein_tls::{Config, Name};
use std::collections::VecDeque;

/// Independent lower output ownership in this bounded proxy (tls.md, 3.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Output {
    Idle,
    Wanted { right: Token, bytes: u32 },
    Granted { right: Token, bytes: u32 },
    Cancelled(Token),
}

/// Actual native TLS and real rustls server, with one prompt backed proxy slot.
/// It does not claim actual IO/kernel capacity or deterministic ciphertext.
/// All public methods check native queue maxima (tls.md, 3.6; testing-strategy.md, 6).
pub struct Pair {
    /// Native client with four distinct obligation cells (tls.md, 3.6).
    pub client: native::Client,
    /// Immutable native read/send/record bounds (tls.md, 3.6).
    pub env: Env<Limits>,
    /// Actual cryptographic peer and authenticated plaintext (tls.md, 3.6).
    pub server: Option<Server>,
    /// Actual authenticated extracted-key peer after a real `TLS1.2` handshake;
    /// the original rustls Server is consumed, never replaced by a fake receiver
    /// (tls.md, 3.6; testing-strategy.md, 2.4).
    pub extracted: Option<Tls12>,
    /// Ciphertext actually emitted by the server, waiting for exact Fill (tls.md, 3.6).
    pub wire: VecDeque<u8>,
    /// The exact unanswered ciphertext read; Room never clears it (tls.md, 3.6).
    pub read: Option<Read>,
    /// Actual lower Finish count for Finish controls (tls.md, 3.6).
    pub finished: u32,
    queued_read: Option<u32>,
    output: Output,
    last_send: Option<(Token, u32)>,
    cap: u32,
    above: Queue<native::Event>,
    below: Queue<native::LowerRequest>,
}

impl Pair {
    /// Make one real peer at fixture-valid wall time. The lower proxy has one
    /// genuine local Send slot and the full checked native cap (tls.md, 3.6).
    #[must_use]
    pub fn new(config: &Config, name: Name, limits: Limits, server: Server) -> Self {
        let cap = native::largest_room(&limits).expect("checked native envelope");
        Self {
            client: native::Client::new(
                config,
                name,
                &limits,
                &native::LowerLimits { read: client::LARGEST_READ, output: cap, sends: 1 },
            )
            .expect("compatible proxy"),
            env: Env { now: Time::ZERO, wall: pki::VALID, limits },
            server: Some(server),
            extracted: None,
            wire: VecDeque::new(),
            read: None,
            finished: 0,
            queued_read: None,
            output: Output::Idle,
            last_send: None,
            cap,
            above: Queue::with_capacity(native::UP_MAX_OUT.above),
            below: Queue::with_capacity(native::UP_MAX_OUT.below),
        }
    }

    /// Send one native upper request, retaining every real lower request and
    /// returning all actual upper observations (tls.md, 3.6).
    pub fn down(&mut self, request: native::Request) -> Vec<native::Event> {
        native::down(&mut self.client, &self.env, request, &mut self.above, &mut self.below);
        self.route(native::DOWN_MAX_OUT)
    }

    /// Forward one actual lower observation from the proxy. A queued Granted
    /// can cross an upper cancellation; a queued Fill box must match the one
    /// actually emitted by `answer`, even across withdrawal (tls.md, 3.6).
    pub fn up(&mut self, event: native::LowerEvent) -> Vec<native::Event> {
        if let native::LowerEvent::Stream(Up::Bytes(bytes)) = &event {
            assert_eq!(
                Some(u32::try_from(bytes.len()).expect("bounded actual Fill winner")),
                self.queued_read.take(),
                "only the proxy's actually emitted exact Fill box is delivered"
            );
        }
        native::up(&mut self.client, &self.env, event, &mut self.above, &mut self.below);
        self.route(native::UP_MAX_OUT)
    }

    /// Poll the independent actual read/output proxy. Emitting a grant commits
    /// its one slot; emitting a Fill moves its box and retains one size witness
    /// until `up`, including after withdrawal (tls.md, 3.6).
    pub fn answer(&mut self) -> Option<native::LowerEvent> {
        if let Some(Read::Fill(bytes)) = self.read
            && self.wire.len() >= usize::try_from(bytes).expect("bounded record")
        {
            assert!(self.queued_read.replace(bytes).is_none(), "one actual queued Fill winner");
            self.read = None;
            let bytes: Vec<u8> = self.wire.drain(..usize::try_from(bytes).expect("bounded Fill")).collect();
            return Some(native::LowerEvent::Stream(Up::Bytes(bytes.into())));
        }
        match self.output {
            Output::Wanted { right, bytes } => {
                self.output = Output::Granted { right, bytes };
                Some(native::LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted }))
            }
            Output::Cancelled(right) => {
                self.output = Output::Idle;
                Some(native::LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Cancelled }))
            }
            Output::Idle | Output::Granted { .. } => None,
        }
    }

    /// Drive only actual proxy answers, at most 10,000 transitions. It may rest
    /// with a genuinely unanswered read (tls.md, 3.6; testing-strategy.md, 6).
    pub fn settle(&mut self) -> Vec<native::Event> {
        let mut events = Vec::new();
        for _ in 0..10_000 {
            let Some(event) = self.answer() else { return events };
            events.extend(self.up(event));
        }
        panic!("bounded prompt native TLS peer rests");
    }

    /// Actual peer records moved onto the wire; no synthetic plaintext answers
    /// or Ready observations (tls.md, 3.6).
    pub fn pull(&mut self) {
        match &mut self.server {
            Some(server) => self.wire.extend(server.transmit()),
            None => self.wire.extend(self.extracted.as_mut().expect("actual extracted peer").transmit()),
        }
    }

    /// Consume the actual completed extractable peer; all future Sends are
    /// authenticated using its unchanged directional keys (tls.md, 3.6).
    pub fn extract_peer(&mut self) {
        assert!(self.extracted.is_none(), "one actual extraction");
        self.extracted = Some(Tls12::new(self.server.take().expect("actual rustls peer")));
    }

    /// Current actual proxy reservation size, independently of its read. Used
    /// to compare growing owed output with the older full grant (tls.md, 3.6).
    #[must_use]
    pub fn output_bytes(&self) -> Option<u32> {
        match self.output {
            Output::Wanted { bytes, .. } | Output::Granted { bytes, .. } => Some(bytes),
            Output::Idle | Output::Cancelled(_) => None,
        }
    }

    /// Only the last actual consumed ciphertext Send's identity and length;
    /// bounded observer state, not predicted or replayable bytes (tls.md, 3.6).
    #[must_use]
    pub fn last_send(&self) -> Option<(Token, u32)> {
        self.last_send
    }

    fn route(&mut self, max: skein_tls::MaxOut) -> Vec<native::Event> {
        assert!(self.above.len() <= max.above && self.below.len() <= max.below);
        while let Some(request) = self.below.pop() {
            match request {
                native::LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 }) => {
                    assert!(
                        self.read.take().is_some() || self.queued_read.is_some(),
                        "withdraw only an actual live Fill or its already emitted winner"
                    );
                }
                native::LowerRequest::Stream(Down::Demand { read, room: 0 }) => {
                    assert!(self.queued_read.is_none(), "queued winner is delivered before a new Fill");
                    assert!(self.read.replace(read).is_none());
                    assert!(matches!(read, Read::Fill(n) if n <= client::LARGEST_READ));
                }
                native::LowerRequest::Stream(Down::Demand { .. } | Down::Send(_)) => panic!("native lower reads only"),
                native::LowerRequest::Stream(Down::Finish) => self.finished += 1,
                native::LowerRequest::Output(OutputDown::Room { right, bytes }) => {
                    assert_eq!(self.output, Output::Idle);
                    assert!(bytes > 0 && bytes <= self.cap);
                    self.output = Output::Wanted { right, bytes };
                }
                native::LowerRequest::Output(OutputDown::Cancel { right }) => match self.output {
                    Output::Wanted { right: pending, .. } if pending == right => self.output = Output::Cancelled(right),
                    Output::Idle | Output::Wanted { .. } | Output::Granted { .. } | Output::Cancelled(_) => {}
                },
                native::LowerRequest::Output(OutputDown::Send { right, bytes }) => {
                    let Output::Granted { right: granted, bytes: cap } = self.output else {
                        panic!("actual backed native Send")
                    };
                    assert_eq!(right, granted);
                    assert!(bytes.len() <= usize::try_from(cap).expect("bounded ciphertext"));
                    self.output = Output::Idle;
                    self.last_send = Some((right, u32::try_from(bytes.len()).expect("bounded actual ciphertext")));
                    match &mut self.server {
                        Some(server) => server.receive(&bytes),
                        None => self.extracted.as_mut().expect("actual extracted peer").receive(&bytes),
                    }
                    self.pull();
                }
                native::LowerRequest::Output(OutputDown::Release { right }) => {
                    assert!(matches!(self.output, Output::Granted { right: actual, .. } if actual == right));
                    self.output = Output::Idle;
                }
            }
        }
        let mut events = Vec::new();
        while let Some(event) = self.above.pop() {
            events.push(event);
        }
        events
    }
}
