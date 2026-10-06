//! Native TLS heap steps against the checked bound (tls.md, 3.6 and 5;
//! testing-strategy.md, 5). The real peer runs between steps. All emitted
//! boxes are copied into reserved harness buffers and dropped before checking;
//! caller plaintext is charged separately. A complete sealed box dropped by
//! an encryption error remains a step allocation, never a handed-out box.

use std::collections::VecDeque;
use std::sync::Arc;

use skein_heap::{Counting, Meter, Span};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Env, Queue, Time, Token};
use skein_tls::client::{self, Error, Limits, native};
use skein_tls_world::native_peer::Tls12;
use skein_tls_world::pki::{self, Chain, Versions};
use skein_tls_world::server::Server;

#[global_allocator]
static HEAP: Counting = Counting;

const SMALL: Limits = Limits { read: 1, send: 1, records: client::MAX_RECORD };
const LARGE: Limits = Limits { read: 40_000, send: 40_000, records: 3 * client::MAX_RECORD };

/// One independent prompt proxy output slot; the actual IO proof lives in
/// `native_output.rs`. No meter step invents a TLS record or cryptographic event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Output {
    Idle,
    Wanted { right: Token, bytes: u32 },
    Granted { right: Token, bytes: u32 },
    Cancelled(Token),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plain {
    Idle,
    Wanted(Token),
    Granted(Token),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Handshake,
    Exchange,
    Closing,
    Closed,
}

#[derive(Debug)]
struct Measured {
    most: u64,
    sealed: usize,
    failed: Option<Error>,
}

/// Genuine controls admitted by pinned rustls, never predicted reply counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Control {
    Idle,
    Keys(u32),
    HelloRequest,
}

/// The server's and harness's heap is independently measured between native
/// steps; no peer allocation is silently counted as TLS memory (tls.md, 5).
struct Harness {
    meter: Meter,
    outside: i64,
    bound: u64,
    most: u64,
    sealed: usize,
    client: Option<native::Client>,
    env: Env<Limits>,
    above: Queue<native::Event>,
    below: Queue<native::LowerRequest>,
    peer: Option<Server>,
    extracted: Option<Tls12>,
    _config: Arc<rustls::server::ServerConfig>,
    wire: VecDeque<u8>,
    outgoing: Vec<u8>,
    request: Vec<u8>,
    response: Vec<u8>,
    received: Vec<u8>,
    sent: usize,
    sequence: u64,
    name_bytes: usize,
    read: Option<Read>,
    output: Output,
    plain: Plain,
    phase: Phase,
    upper_read: bool,
    responded: bool,
    corrupt: bool,
    control: Control,
    failed: Option<Error>,
}

impl Harness {
    fn down(&mut self, request: native::Request) {
        let extra = match &request {
            native::Request::Output(OutputDown::Send { bytes, .. }) => bytes.len() as u64,
            native::Request::Client(_) | native::Request::Output(_) => 0,
        };
        self.meter.start();
        native::down(self.client.as_mut().expect("made"), &self.env, request, &mut self.above, &mut self.below);
        let step = self.meter.end();
        self.take(native::DOWN_MAX_OUT);
        self.check(step, extra);
    }

    fn up(&mut self, event: native::LowerEvent) {
        self.meter.start();
        native::up(self.client.as_mut().expect("made"), &self.env, event, &mut self.above, &mut self.below);
        let step = self.meter.end();
        self.take(native::UP_MAX_OUT);
        self.check(step, 0);
    }

    fn check(&mut self, step: skein_heap::Measured, extra: u64) {
        let bound = i64::try_from(self.bound + extra).expect("finite") + self.outside;
        let own = self.meter.check(step, u64::try_from(bound).expect("positive"), &self.phase);
        let own = i64::try_from(own).expect("finite") - self.outside - i64::try_from(extra).expect("finite");
        self.most = self.most.max(u64::try_from(own).expect("positive"));
    }

    fn take(&mut self, maximum: skein_tls::MaxOut) {
        assert!(self.above.len() <= maximum.above && self.below.len() <= maximum.below);
        while let Some(event) = self.above.pop() {
            match event {
                native::Event::Client(client::Event::Ready(_)) => self.phase = Phase::Exchange,
                native::Event::Client(client::Event::Stream(Up::Bytes(bytes))) => {
                    assert!(self.upper_read && self.received.len() + bytes.len() <= self.received.capacity());
                    self.upper_read = false;
                    self.received.extend_from_slice(&bytes);
                    drop(bytes);
                }
                native::Event::Client(client::Event::Stream(Up::Failed(_))) => self.upper_read = false,
                native::Event::Client(client::Event::Failed(error)) => self.failed = Some(error),
                native::Event::Client(client::Event::Closed) => self.phase = Phase::Closed,
                native::Event::Output(OutputUp::Settled { right, outcome }) => {
                    assert_eq!(self.plain, Plain::Wanted(right));
                    self.plain = match outcome {
                        OutputOutcome::Granted => Plain::Granted(right),
                        OutputOutcome::Cancelled | OutputOutcome::Failed(_) => Plain::Idle,
                    };
                }
                native::Event::Client(client::Event::Stream(Up::End | Up::Room))
                | native::Event::TransportClosing
                | native::Event::TransportClosed => panic!("meter proxy retains exact native lifecycle"),
            }
        }
        while let Some(request) = self.below.pop() {
            self.take_lower(request);
        }
    }

    fn take_lower(&mut self, request: native::LowerRequest) {
        match request {
            native::LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 }) => self.read = None,
            native::LowerRequest::Stream(Down::Demand { read, room: 0 }) => {
                assert!(self.read.replace(read).is_none(), "one independent ciphertext read");
            }
            native::LowerRequest::Output(OutputDown::Room { right, bytes }) => {
                assert_eq!(self.output, Output::Idle);
                assert!(bytes <= native::largest_room(&self.env.limits).expect("compatible"));
                self.output = Output::Wanted { right, bytes };
            }
            native::LowerRequest::Output(OutputDown::Send { right, bytes }) => {
                let Output::Granted { right: granted, bytes: cap } = self.output else { panic!("real proxy grant") };
                assert_eq!(right, granted);
                assert!(bytes.len() <= usize::try_from(cap).expect("finite"));
                assert!(self.outgoing.len() + bytes.len() <= self.outgoing.capacity());
                self.sealed = self.sealed.max(bytes.len());
                self.outgoing.extend_from_slice(&bytes);
                drop(bytes);
                self.output = Output::Idle;
            }
            native::LowerRequest::Output(OutputDown::Release { right }) => {
                assert!(matches!(self.output, Output::Granted { right: granted, .. } if granted == right));
                self.output = Output::Idle;
            }
            native::LowerRequest::Output(OutputDown::Cancel { right }) => match self.output {
                Output::Wanted { right: pending, .. } if pending == right => self.output = Output::Cancelled(right),
                Output::Idle | Output::Wanted { .. } | Output::Granted { .. } | Output::Cancelled(_) => {}
            },
            native::LowerRequest::Stream(Down::Demand { .. } | Down::Send(_) | Down::Finish) => {
                panic!("read-only native lower proxy")
            }
        }
    }

    fn peer_received(&self) -> &[u8] {
        match &self.peer {
            Some(peer) => &peer.received,
            None => &self.extracted.as_ref().expect("actual extracted peer").received,
        }
    }

    fn pull(&mut self) {
        match &mut self.peer {
            Some(peer) => self.wire.extend(peer.transmit()),
            None => self.wire.extend(self.extracted.as_mut().expect("actual extracted peer").transmit()),
        }
    }

    fn control(&mut self) {
        if self.sent != 0 || !matches!(self.plain, Plain::Granted(_)) {
            return;
        }
        match std::mem::replace(&mut self.control, Control::Idle) {
            Control::Idle => {}
            Control::Keys(count) => {
                for _ in 0..count {
                    let peer = self.peer.as_mut().expect("actual TLS1.3 peer");
                    peer.key_update();
                    let requesting_record = peer.transmit();
                    assert_eq!(requesting_record.len(), 27, "each of32 actual requesting KeyUpdate records is emitted");
                    self.wire.extend(requesting_record);
                }
            }
            Control::HelloRequest => {
                let mut peer = Tls12::new(self.peer.take().expect("actual completed extractable TLS1.2 server"));
                peer.hello_request();
                self.extracted = Some(peer);
                self.pull();
            }
        }
    }

    fn between(&mut self) {
        let span = Span::start();
        if !self.outgoing.is_empty() {
            match &mut self.peer {
                Some(peer) => peer.receive(&self.outgoing),
                None => self.extracted.as_mut().expect("actual extracted peer").receive(&self.outgoing),
            }
            self.outgoing.clear();
        }
        self.control();
        if !self.responded && self.peer_received().len() == self.request.len() {
            assert_eq!(self.peer_received(), self.request, "actual authenticated maximal caller payload");
            self.responded = true;
            if let Some(peer) = &mut self.peer {
                peer.write(&self.response);
            } else {
                let peer = self.extracted.as_mut().expect("actual extracted peer");
                assert_eq!(peer.alerts, [[1, 100]], "one authenticated actual refusal before full plaintext");
                assert_eq!(peer.records.first(), Some(&rustls::ContentType::Alert), "actual owed prefix first");
                peer.write(&self.response);
            }
            let start = self.wire.len();
            self.pull();
            if self.corrupt {
                let body = usize::from(u16::from_be_bytes([self.wire[start + 3], self.wire[start + 4]]));
                self.wire[start + 4 + body] ^= 1;
            }
        }
        self.pull();
        self.outside += span.end().net;
    }

    fn answer(&mut self) -> bool {
        if let Some(Read::Fill(count)) = self.read {
            let count = usize::try_from(count).expect("finite");
            if self.wire.len() >= count {
                self.read = None;
                // Delivery made outside the step, but charged to TLS work:
                // it is not added to outside. It is dropped before processing.
                let bytes: Vec<u8> = self.wire.drain(..count).collect();
                self.up(native::LowerEvent::Stream(Up::Bytes(bytes.into())));
                return true;
            }
        }
        match self.output {
            Output::Wanted { right, bytes } => {
                self.output = Output::Granted { right, bytes };
                self.up(native::LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted }));
                true
            }
            Output::Cancelled(right) => {
                self.output = Output::Idle;
                self.up(native::LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Cancelled }));
                true
            }
            Output::Idle | Output::Granted { .. } => false,
        }
    }

    fn next(&mut self) {
        if self.answer() {
            return;
        }
        if self.phase == Phase::Closing {
            assert_eq!(self.output, Output::Idle, "all actual output obligations settled before physical Closed");
            self.up(native::LowerEvent::Closed);
            return;
        }
        if self.failed.is_some() || self.received.len() == self.response.len() {
            if self.failed.is_none() {
                assert_eq!(self.received, self.response, "actual complete maximal intake payload");
            }
            self.phase = Phase::Closing;
            self.down(native::Request::Client(client::Request::Close));
            return;
        }
        assert_eq!(self.phase, Phase::Exchange, "actual handshake must retain lower work");
        if !self.upper_read {
            self.ask_read();
            return;
        }
        match self.plain {
            Plain::Granted(right) => {
                let count = usize::try_from(self.env.limits.send).expect("finite").min(self.request.len() - self.sent);
                let bytes: Box<[u8]> = self.request[self.sent..self.sent + count].into();
                self.sent += count;
                self.plain = Plain::Idle;
                self.down(native::Request::Output(OutputDown::Send { right, bytes }));
            }
            Plain::Idle if self.sent < self.request.len() => {
                let right = Token::new(self.sequence);
                self.sequence = self.sequence.checked_add(1).expect("bounded case");
                self.plain = Plain::Wanted(right);
                self.down(native::Request::Output(OutputDown::Room { right, bytes: self.env.limits.send }));
            }
            Plain::Idle | Plain::Wanted(_) => panic!("independent metered TLS rights retain work"),
        }
    }

    fn ask_read(&mut self) {
        self.upper_read = true;
        let count =
            usize::try_from(self.env.limits.read).expect("finite").min(self.response.len() - self.received.len());
        self.down(native::Request::Client(client::Request::Stream(Down::Demand {
            read: Read::Fill(u32::try_from(count).expect("finite")),
            room: 0,
        })));
    }

    fn finish(mut self) -> Measured {
        let held = i64::try_from(self.meter.held()).expect("finite") - self.outside;
        let span = Span::start();
        drop(self.client.take().expect("made"));
        let freed = span.end().net;
        let name = i64::try_from(self.name_bytes).expect("finite name");
        assert_eq!(held + freed, -name, "all native owned heap, and only the moved caller Name, is released");
        Measured { most: self.most, sealed: self.sealed, failed: self.failed }
    }
}

fn measure(limits: Limits, versions: Versions, chain: Chain, corrupt: bool) -> Measured {
    let server = pki::Server { versions, chain, extractable: versions == Versions::Tls12, ..pki::Server::plain() };
    let config = pki::client(&[]);
    let text = if chain == Chain::Big { "big.skein.test" } else { "skein.test" };
    let name = skein_tls::Name::new(text).expect("name");
    let cap = native::largest_room(&limits).expect("checked envelope");
    let server_config = server.config();
    let mut harness = Harness {
        meter: Meter::new(),
        outside: 0,
        bound: native::worst_case(&limits).expect("checked bound"),
        most: 0,
        sealed: 0,
        client: None,
        env: Env { now: Time::ZERO, wall: pki::VALID, limits },
        above: Queue::with_capacity(native::UP_MAX_OUT.above),
        below: Queue::with_capacity(native::UP_MAX_OUT.below),
        peer: Some(Server::new(Arc::clone(&server_config))),
        extracted: None,
        _config: server_config,
        wire: VecDeque::with_capacity(4 * 65_536),
        outgoing: Vec::with_capacity(1 << 20),
        request: vec![b'p'; if limits.send == 1 { 300 } else { 100_000 }],
        response: vec![b'r'; if limits.read == 1 { 2_000 } else { 100_000 }],
        received: Vec::with_capacity(100_001),
        sent: 0,
        sequence: 0,
        name_bytes: text.len(),
        read: None,
        output: Output::Idle,
        plain: Plain::Idle,
        phase: Phase::Handshake,
        upper_read: false,
        responded: false,
        corrupt,
        control: if versions == Versions::Tls13 && limits == LARGE {
            Control::Keys(32)
        } else if versions == Versions::Tls12 {
            Control::HelloRequest
        } else {
            Control::Idle
        },
        failed: None,
    };
    harness.meter = Meter::new();
    harness.meter.start();
    let client = native::Client::new(
        &config,
        name,
        &limits,
        &native::LowerLimits { read: client::LARGEST_READ, output: cap, sends: 1 },
    )
    .expect("compatible lower configuration");
    let step = harness.meter.end();
    harness.check(step, 0);
    harness.client = Some(client);
    harness.down(native::Request::Client(client::Request::Handshake));
    for _ in 0..100_000 {
        if harness.phase == Phase::Closed {
            return harness.finish();
        }
        harness.between();
        harness.next();
    }
    panic!("metered native conversation settles");
}

#[test]
fn native_full_payload_intake_and_complete_ciphertext_are_metered_at_original_small_and_large_limits() {
    for versions in [Versions::Tls12, Versions::Tls13] {
        for limits in [SMALL, LARGE] {
            let measured = measure(limits, versions, Chain::Leaf, false);
            assert_eq!(measured.failed, None, "{versions:?} {limits:?} {measured:?}");
            assert!(measured.most > u64::from(limits.read + client::MAX_PLAINTEXT + limits.records + client::FLIGHT));
            if limits == LARGE {
                assert!(measured.sealed >= 40_000, "actual maximal encrypted caller payload: {measured:?}");
                assert_eq!(
                    measured.sealed,
                    match versions {
                        Versions::Tls13 => 40_000 + 3 * 22 + 27,
                        Versions::Tls12 => 31 + 40_000 + 3 * 29,
                        Versions::Both => unreachable!("explicit version cases"),
                    },
                    "complete actual requesting-control response plus maximal plaintext: {measured:?}"
                );
            }
        }
    }
}

#[test]
fn native_big_chain_and_actual_decrypt_error_keep_full_retention_and_release() {
    for versions in [Versions::Tls12, Versions::Tls13] {
        let measured = measure(LARGE, versions, Chain::Big, false);
        assert_eq!(measured.failed, None, "{measured:?}");
        let buffers = u64::from(LARGE.read + client::MAX_PLAINTEXT + LARGE.records + client::FLIGHT);
        assert!(measured.most > buffers + 2 * 38_000, "actual double held decoded chain: {measured:?}");
        assert_eq!(measure(LARGE, versions, Chain::Leaf, true).failed, Some(Error::Decrypt));
    }
}

#[test]
fn real_queued_fill_boxes_drop_exactly_after_withdrawal_or_finish_without_step_allocation() {
    use skein_tls_world::native_pair::Pair;
    for versions in [Versions::Tls12, Versions::Tls13] {
        for body in [false, true] {
            for finish in [false, true] {
                let server = pki::Server { versions, ..pki::Server::plain() };
                let server_config = server.config();
                let ticket_records = server_config.send_tls13_tickets;
                let mut pair = Pair::new(&pki::client(&[]), pki::name(), LARGE, Server::new(server_config));
                let mut events = pair.down(native::Request::Client(client::Request::Handshake));
                events.extend(pair.settle());
                assert!(matches!(&events[..], [native::Event::Client(client::Event::Ready(_))]));
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Fill(LARGE.read),
                        room: 0
                    })))
                    .is_empty()
                );
                // Caller-owned full heap payload stays live outside the native
                // delivery Span; no large stack array or uncharged step copy.
                let plaintext = vec![b'x'; 40_000];
                pair.server.as_mut().expect("actual peer").write(&plaintext);
                pair.pull();
                let winner = if body {
                    queued_maximal_body(&mut pair, versions, ticket_records)
                } else {
                    pair.answer().expect("actual emitted Header Fill")
                };
                let native::LowerEvent::Stream(Up::Bytes(bytes)) = &winner else {
                    panic!("real owning queued Fill box")
                };
                let count = i64::try_from(bytes.len()).expect("bounded Fill");
                assert!(count > 0 && count <= i64::from(client::LARGEST_READ));
                if body {
                    assert!(
                        count >= i64::from(client::MAX_PLAINTEXT),
                        "actual maximum record body pressure: {versions:?}, body={body}, finish={finish}, count={count}"
                    );
                }
                assert!(pair.read.is_none(), "box ownership moved out of actual Fill");
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Nothing,
                        room: 0
                    })))
                    .is_empty()
                );
                if finish {
                    assert!(pair.down(native::Request::Client(client::Request::Stream(Down::Finish))).is_empty());
                }
                let span = Span::start();
                assert!(pair.up(winner).is_empty(), "disposed winner emits no synthetic observation");
                let step = span.end();
                assert_eq!(step.net, -count, "only the actual caller-owned queued ciphertext box is released");
                assert_eq!(step.peak, 0, "withdrawn winner is dropped without copying or processing");
                assert!(pair.read.is_none());
                assert_eq!(plaintext.len(), 40_000, "full caller-owned payload retained throughout disposal");
                drop(plaintext);
            }
        }
    }
}

/// Process every genuine pre-data ticket record, then retain an actual maximal
/// Body winner. The real configured ticket count bounds the prefix; neither
/// peer tickets nor TLS controls are suppressed (tls.md, 3.6; testing-strategy.md, 2.4).
fn queued_maximal_body(
    pair: &mut skein_tls_world::native_pair::Pair,
    versions: Versions,
    ticket_records: usize,
) -> native::LowerEvent {
    let records = ticket_records.checked_add(1).expect("configured ticket prefix plus first data record");
    for prefix in 0..records {
        let header = pair.answer().expect("actual emitted Header before maximal Body");
        let native::LowerEvent::Stream(Up::Bytes(bytes)) = &header else { panic!("actual Header Fill box") };
        assert_eq!(bytes.len(), usize::try_from(client::HEADER).expect("Header"));
        let length = u32::from(u16::from_be_bytes([bytes[3], bytes[4]]));
        assert!(pair.up(header).is_empty(), "Header cannot fabricate a plaintext or output answer");
        assert_eq!(pair.read, Some(Read::Fill(length)), "exact actual next Body Fill");
        let winner = pair.answer().expect("actual emitted Body Fill");
        let native::LowerEvent::Stream(Up::Bytes(bytes)) = &winner else { panic!("actual Body Fill box") };
        assert_eq!(bytes.len(), usize::try_from(length).expect("bounded Body"));
        if length >= client::MAX_PLAINTEXT {
            if versions == Versions::Tls13 {
                assert!(prefix > 0, "actual post-handshake ticket flight preceded the full peer data");
            }
            return winner;
        }
        assert_eq!(versions, Versions::Tls13, "only the actual TLS13 ticket flight precedes maximal data");
        assert!(
            pair.up(winner).is_empty(),
            "actual ticket record is processed, never discarded or a short Fill answer"
        );
    }
    panic!("actual configured ticket flight followed by maximal40000 data must yield a maximum Body: {versions:?}");
}
