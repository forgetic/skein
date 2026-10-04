//! The client's worst case against the counting allocator (tls.md, 5;
//! programming-model.md, 6.3): every call of an entry point a step of the
//! meter, and what the client held of its own, rustls's heap included,
//! never more than its `worst_case`.
//!
//! The server runs on the same thread, between the client's steps, and
//! allocates as it goes. Its heap, and the harness's, is measured with a
//! span around everything done between steps, and each step is checked
//! against the client's bound plus that heap, which no step of the client's
//! touches: what the meter finds past it is the client's own. An input moved
//! into a step was counted by whoever made it (testing.md, 5): a delivery
//! from below is made between steps, to the client's demand, and its worst
//! case covers it; a `Send` from above is the side above's, and the step
//! that takes it is checked against the bound and its size. What a step
//! emits is handed out: its bytes copied into buffers reserved beforehand,
//! and dropped before the check.

use std::collections::VecDeque;
use std::sync::Arc;

use rustls::server::ServerConfig;

use skein_heap::{Counting, Meter, Span};
use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Queue, Rng, Time};
use skein_tls::client::{self, Client, Error, Event, Limits, Request};
use skein_tls::{Config, Name};
use skein_tls_world::pki::{self, Chain, Versions};
use skein_tls_world::server::Server;
use skein_tls_world::world;

#[global_allocator]
static HEAP: Counting = Counting;

/// How a case ends, besides its exchange.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ending {
    /// The server sends `close_notify` after its response; the side above
    /// reads to the end, then closes.
    CloseNotify,
    /// The stream ends after the response without it.
    Truncate,
    /// A byte of the response's records is changed on the way.
    Corrupt,
    /// The side above closes after this many steps.
    Close(u32),
    /// The stream below fails after this many steps.
    Fail(u32),
}

#[derive(Clone, Debug)]
struct Case {
    server: pki::Server,
    name: &'static str,
    alpn: Vec<Vec<u8>>,
    limits: Limits,
    request: usize,
    response: usize,
    key_update: bool,
    ending: Ending,
}

impl Case {
    fn plain(limits: Limits, request: usize, response: usize) -> Case {
        Case {
            server: pki::Server::plain(),
            name: "skein.test",
            alpn: Vec::new(),
            limits,
            request,
            response,
            key_update: false,
            ending: Ending::CloseNotify,
        }
    }
}

/// What a case came to: the most the client held of its own in a step.
#[derive(Debug)]
struct Measured {
    most: u64,
    bound: u64,
    failed: Option<Error>,
    limits: Limits,
}

/// What the client held past its own buffers, at its most: rustls's heap,
/// and the delivery it read.
fn past(measured: &Measured) -> i64 {
    let limits = measured.limits;
    let buffers = u64::from(limits.read) + 16_384 + u64::from(limits.records) + u64::from(client::FLIGHT);
    i64::try_from(measured.most).expect("fits") - i64::try_from(buffers).expect("fits")
}

/// The side above's side of the plaintext stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Face {
    Idle,
    Demanded(Read, u32),
    Granted(u32),
    Over,
}

#[expect(clippy::struct_excessive_bools, reason = "the case's state between steps, a flag each")]
struct Harness {
    case: Case,
    meter: Meter,
    /// The heap made outside the client's steps since the meter's base.
    outside: i64,
    bound: u64,
    most: u64,
    client: Option<Client>,
    env: Env<Limits>,
    events: Queue<Event>,
    requests: Queue<Down>,
    server: Server,
    /// Kept for the whole case: rustls's server lets its configuration go
    /// once its handshake is done, which was made before the meter.
    _server_config: Arc<ServerConfig>,
    request: Vec<u8>,
    response: Vec<u8>,
    responded: bool,
    wire: VecDeque<u8>,
    /// The client's sends, until the server reads them: reserved.
    outgoing: Vec<u8>,
    demand: Option<(Read, u32)>,
    eof: bool,
    ended: bool,
    face: Face,
    started: bool,
    sent: usize,
    /// What the side above read: reserved.
    received: Vec<u8>,
    end: bool,
    failed: Option<Error>,
    closing: bool,
    closed: bool,
    steps: u32,
}

/// Runs `case`, every step checked against the client's worst case.
fn measure(case: &Case, seed: u64) -> Measured {
    let mut rng = Rng::new(seed);
    let alpn: Vec<&[u8]> = case.alpn.iter().map(Vec::as_slice).collect();
    let config: Config = pki::client(&alpn);
    let name = Name::new(case.name).expect("a name");
    let bound = client::worst_case(&case.limits).expect("the limits are honoured");
    let server_config = case.server.config();
    let mut harness = Harness {
        case: case.clone(),
        meter: Meter::new(),
        outside: 0,
        bound,
        most: 0,
        client: None,
        env: Env { now: Time::ZERO, wall: pki::VALID, limits: case.limits },
        events: Queue::with_capacity(8),
        requests: Queue::with_capacity(8),
        server: Server::new(Arc::clone(&server_config)),
        _server_config: server_config,
        request: world::text(&mut rng, case.request),
        response: world::text(&mut rng, case.response),
        responded: false,
        wire: VecDeque::with_capacity(4 * 65_536),
        outgoing: Vec::with_capacity(1 << 20),
        demand: None,
        eof: false,
        ended: false,
        face: Face::Idle,
        started: false,
        sent: 0,
        received: Vec::with_capacity(case.response + 1),
        end: false,
        failed: None,
        closing: false,
        closed: false,
        steps: 0,
    };
    harness.meter = Meter::new();
    // Making the client is a step of its own.
    harness.meter.start();
    let made = Client::new(&config, name, &case.limits);
    let step = harness.meter.end();
    harness.check(step, 0);
    harness.client = Some(made);
    for _ in 0..1_000_000 {
        if harness.closed {
            return harness.finish();
        }
        harness.between();
        let input = harness.next(&mut rng);
        harness.step(input);
    }
    panic!("{case:?}: a case ends");
}

enum Input {
    Up(Up),
    Down(Request),
}

impl Harness {
    /// A step of the client's, checked.
    fn step(&mut self, input: Input) {
        self.steps += 1;
        let extra = match &input {
            Input::Down(Request::Stream(Down::Send(bytes))) => bytes.len() as u64,
            Input::Up(_) | Input::Down(_) => 0,
        };
        let client = self.client.as_mut().expect("made");
        self.meter.start();
        match input {
            Input::Up(ev) => client::up(client, &self.env, ev, &mut self.events, &mut self.requests),
            Input::Down(rq) => client::down(client, &self.env, rq, &mut self.events, &mut self.requests),
        }
        let step = self.meter.end();
        self.take();
        self.check(step, extra);
    }

    fn check(&mut self, step: skein_heap::Measured, extra: u64) {
        let bound = i64::try_from(self.bound + extra).expect("fits") + self.outside;
        let what = format!("{:?}, step {}", self.case, self.steps);
        let own = self.meter.check(step, u64::try_from(bound).expect("positive"), &what);
        let own = i64::try_from(own).expect("fits") - self.outside - i64::try_from(extra).expect("fits");
        self.most = self.most.max(u64::try_from(own).expect("positive"));
    }

    /// What a step emitted, taken: payloads copied into reserved buffers,
    /// and dropped, handed out.
    fn take(&mut self) {
        while let Some(event) = self.events.pop() {
            match event {
                Event::Ready(agreed) => drop(agreed),
                Event::Stream(Up::Bytes(bytes)) => {
                    assert!(self.received.len() + bytes.len() <= self.received.capacity(), "reserved");
                    self.received.extend_from_slice(&bytes);
                    drop(bytes);
                    self.face = Face::Idle;
                }
                Event::Stream(Up::Room) => {
                    let Face::Demanded(_, room) = self.face else { panic!("room for a demand") };
                    self.face = Face::Granted(room);
                }
                Event::Stream(Up::End) => {
                    self.end = true;
                    self.face = Face::Idle;
                }
                Event::Stream(Up::Failed(_)) => self.face = Face::Over,
                Event::Failed(error) => self.failed = Some(error),
                Event::Closed => self.closed = true,
            }
        }
        while let Some(request) = self.requests.pop() {
            match request {
                Down::Demand { read: Read::Nothing, room: 0 } => self.demand = None,
                Down::Demand { read, room } => self.demand = Some((read, room)),
                Down::Send(bytes) => {
                    assert!(self.outgoing.len() + bytes.len() <= self.outgoing.capacity(), "reserved");
                    self.outgoing.extend_from_slice(&bytes);
                    drop(bytes);
                }
                Down::Finish => {}
            }
        }
    }

    /// Everything done between steps: the server's, measured.
    fn between(&mut self) {
        let span = Span::start();
        if !self.outgoing.is_empty() {
            self.server.receive(&self.outgoing);
            self.outgoing.clear();
        }
        if !self.responded && !self.server.handshaking() && self.server.received.len() >= self.request.len() {
            self.responded = true;
            let half = self.response.len() / 2;
            self.server.write(&self.response[..half]);
            if self.case.key_update && self.case.server.versions != Versions::Tls12 {
                self.server.key_update();
            }
            self.server.write(&self.response[half..]);
            let start = self.wire.len();
            self.wire.extend(self.server.transmit());
            match self.case.ending {
                Ending::CloseNotify => {
                    self.server.close_notify();
                    self.eof = true;
                }
                Ending::Truncate => self.eof = true,
                Ending::Corrupt => {
                    // The last byte of the response's first record.
                    let length = usize::from(u16::from_be_bytes([self.wire[start + 3], self.wire[start + 4]]));
                    self.wire[start + 4 + length] ^= 1;
                    self.eof = true;
                }
                Ending::Close(_) | Ending::Fail(_) => {}
            }
        }
        self.wire.extend(self.server.transmit());
        self.outside += span.end().net;
    }

    /// The next input: an interruption, the side below's answer, or the side
    /// above's next move.
    fn next(&mut self, rng: &mut Rng) -> Input {
        match self.case.ending {
            Ending::Close(at) if self.steps >= at && !self.closing => {
                self.closing = true;
                return Input::Down(Request::Close);
            }
            Ending::Fail(at) if self.steps >= at && !self.ended => {
                self.ended = true;
                self.demand = None;
                return Input::Up(Up::Failed(Fault::Reset));
            }
            Ending::CloseNotify | Ending::Truncate | Ending::Corrupt | Ending::Close(_) | Ending::Fail(_) => {}
        }
        if let Some((read, room)) = self.demand {
            if let Read::Fill(n) = read {
                let n = usize::try_from(n).expect("fits");
                if !self.ended && self.wire.len() >= n {
                    self.demand = None;
                    // The delivery, made between steps: the client's to count.
                    let mut bytes = Vec::with_capacity(n);
                    bytes.extend(self.wire.drain(..n));
                    return Input::Up(Up::Bytes(bytes.into_boxed_slice()));
                }
            }
            if room > 0 {
                self.demand = None;
                return Input::Up(Up::Room);
            }
            if self.eof && !self.ended {
                self.ended = true;
                self.demand = None;
                return Input::Up(Up::End);
            }
        }
        if !self.started {
            self.started = true;
            return Input::Down(Request::Handshake);
        }
        let left = self.request.len() - self.sent;
        let limits = self.case.limits;
        match self.face {
            Face::Granted(room) => {
                let len = usize::try_from(room).expect("fits").min(left);
                let piece: Box<[u8]> = self.request[self.sent..self.sent + len].into();
                self.sent += len;
                self.face = Face::Idle;
                Input::Down(Request::Stream(Down::Send(piece)))
            }
            Face::Idle if !self.end && self.failed.is_none() && !self.closing => {
                let unread = self.response.len() - self.received.len();
                let most = u32::try_from(unread).unwrap_or(u32::MAX).min(limits.read).max(1);
                let read = if rng.chance(300) { Read::Line { max: most } } else { Read::Fill(most) };
                let room = u32::try_from(left).unwrap_or(u32::MAX).min(limits.send);
                self.face = Face::Demanded(read, room);
                Input::Down(Request::Stream(Down::Demand { read, room }))
            }
            Face::Idle | Face::Demanded(..) | Face::Over if !self.closing => {
                self.closing = true;
                Input::Down(Request::Close)
            }
            Face::Idle | Face::Demanded(..) | Face::Over => panic!("{:?}: stuck at step {}", self.case, self.steps),
        }
    }

    /// The case is over: the client, dropped, frees all it held.
    fn finish(mut self) -> Measured {
        let held = i64::try_from(self.meter.held()).expect("fits") - self.outside;
        let span = Span::start();
        let client = self.client.take().expect("made");
        drop(client);
        let freed = span.end().net;
        // And the name moved into it, the caller's (testing.md, 5), freed
        // with rustls's connection.
        let name = i64::try_from(self.case.name.len()).expect("fits");
        assert_eq!(held + freed, -name, "{:?}: the client frees what it held, and only that", self.case);
        Measured { most: self.most, bound: self.bound, failed: self.failed, limits: self.case.limits }
    }
}

const SMALL: Limits = Limits { read: 1, send: 1, records: client::MAX_RECORD };
const LARGE: Limits = Limits { read: 40_000, send: 40_000, records: 3 * client::MAX_RECORD };

#[test]
fn handshakes_and_exchanges_at_their_limits() {
    for versions in [Versions::Tls13, Versions::Tls12] {
        // Records of 16 KB each way, three to a send, and a key update.
        let mut large = Case::plain(LARGE, 100_000, 100_000);
        large.server.versions = versions;
        large.key_update = true;
        let measured = measure(&large, 1);
        assert_eq!(measured.failed, None, "{large:?}");
        assert!(past(&measured) > 16_384, "{versions:?}: a record's work, past the buffers: {measured:?}");
        // A byte at a time each way.
        let mut small = Case::plain(SMALL, 300, 2_000);
        small.server.versions = versions;
        assert_eq!(measure(&small, 1).failed, None, "{small:?}");
    }
}

#[test]
fn a_retry_and_the_longest_alpn_list() {
    let mut retry = Case::plain(LARGE, 1_000, 1_000);
    retry.server.retry = true;
    retry.alpn = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    retry.server.alpn = vec![b"http/1.1".to_vec()];
    assert_eq!(measure(&retry, 2).failed, None);
    // 64 protocols of three bytes: ALPN's bytes in all.
    let mut alpn = Case::plain(LARGE, 1_000, 1_000);
    alpn.alpn = (0..64).map(|n| format!("p{n:02}").into_bytes()).collect();
    alpn.server.alpn = vec![b"p63".to_vec()];
    assert_eq!(measure(&alpn, 2).failed, None);
}

#[test]
fn the_longest_chain_the_records_hold_is_held_twice() {
    for versions in [Versions::Tls13, Versions::Tls12] {
        for records in [3 * client::MAX_RECORD, 4 * client::MAX_RECORD] {
            let mut big = Case::plain(Limits { records, ..LARGE }, 1_000, 1_000);
            big.server.chain = Chain::Big;
            big.server.versions = versions;
            big.name = "big.skein.test";
            let measured = measure(&big, 3);
            assert_eq!(measured.failed, None, "{big:?}");
            // About 39 KB of certificates, twice, past the buffers.
            assert!(past(&measured) > 2 * 38_000, "{big:?}: {measured:?}");
        }
    }
    let mut short = Case::plain(SMALL, 10, 10);
    short.server.chain = Chain::Big;
    short.name = "big.skein.test";
    assert_eq!(measure(&short, 4).failed, Some(Error::TooLong));
}

#[test]
fn failures_and_closes_in_every_state() {
    for versions in [Versions::Tls13, Versions::Tls12] {
        for (ending, error) in [(Ending::Truncate, Error::Truncated), (Ending::Corrupt, Error::Decrypt)] {
            let mut case = Case::plain(LARGE, 1_000, 50_000);
            case.server.versions = versions;
            case.ending = ending;
            assert_eq!(measure(&case, 5).failed, Some(error), "{case:?}");
        }
        for at in 0..30 {
            for ending in [Ending::Close(at), Ending::Fail(at)] {
                let mut case = Case::plain(LARGE, 1_000, 20_000);
                case.server.versions = versions;
                case.ending = ending;
                let _: Measured = measure(&case, 6);
            }
        }
    }
}

/// The client handed `message` as the server's first, in plaintext records
/// of 16 KB, every call a step of the meter: rustls decodes a handshake
/// message whole before it judges it, in TLS 1.2's forms until a version is
/// agreed.
fn hostile(limits: Limits, message: &[u8]) -> Measured {
    let config = pki::client(&[]);
    let name = Name::new("skein.test").expect("a name");
    let env = Env { now: Time::ZERO, wall: pki::VALID, limits };
    let bound = client::worst_case(&limits).expect("the limits are honoured");
    let mut pieces = Vec::new();
    for chunk in message.chunks(16_384) {
        let mut header = vec![22, 3, 3];
        header.extend_from_slice(&u16::try_from(chunk.len()).expect("fits").to_be_bytes());
        pieces.push(header);
        pieces.push(chunk.to_vec());
    }
    let mut events = Queue::with_capacity(4);
    let mut requests = Queue::with_capacity(4);
    let meter = Meter::new();
    let mut most = 0;
    meter.start();
    let mut tls = Client::new(&config, name, &limits);
    let step = meter.end();
    most = most.max(meter.check(step, bound, &limits));
    let mut failed = None;
    let mut inputs = vec![Input::Down(Request::Handshake), Input::Up(Up::Room)];
    inputs.extend(pieces.iter().map(|piece| Input::Up(Up::Bytes(piece.as_slice().into()))));
    for input in inputs {
        meter.start();
        match input {
            Input::Up(ev) => client::up(&mut tls, &env, ev, &mut events, &mut requests),
            Input::Down(rq) => client::down(&mut tls, &env, rq, &mut events, &mut requests),
        }
        let step = meter.end();
        while let Some(event) = events.pop() {
            if let Event::Failed(error) = event {
                failed = Some(error);
            }
        }
        while let Some(request) = requests.pop() {
            drop(request);
        }
        most = most.max(meter.check(step, bound, &limits));
        if failed.is_some() {
            break;
        }
    }
    Measured { most, bound, failed, limits }
}

/// A handshake message of `kind` and `body`.
fn message(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![kind];
    message.extend_from_slice(&u32::try_from(body.len()).expect("fits").to_be_bytes()[1..]);
    message.extend_from_slice(body);
    message
}

/// A list, after its length of `prefix` bytes, of as many items of `width`
/// bytes as fit `room` bytes in all, the `n`th made by `item(n)`.
fn list(room: usize, prefix: usize, width: usize, item: impl Fn(usize) -> Vec<u8>) -> Vec<u8> {
    let items: Vec<u8> = (0..(room - prefix) / width).flat_map(item).collect();
    let mut list = u32::try_from(items.len()).expect("fits").to_be_bytes()[4 - prefix..].to_vec();
    list.extend_from_slice(&items);
    list
}

/// The hostile messages a server may send first, in the clear, each of at
/// most `size` bytes: a chain of empty certificates (RFC 5246, 7.4.2), a
/// request of one-byte names (7.4.4), and a hello of unknown extensions
/// (7.4.1.3), each of the fewest bytes a decoded element takes.
fn hostile_messages(size: usize) -> [(&'static str, Vec<u8>); 3] {
    let room = size - 4;
    let certificates = message(11, &list(room, 3, 3, |_| vec![0, 0, 0]));
    let mut request = vec![1, 1, 0, 2, 4, 3];
    request.extend(list((room - request.len()).min(65_537), 2, 3, |_| vec![0, 1, 0x30]));
    let mut hello = vec![3, 3];
    hello.extend_from_slice(&[7; 32]);
    hello.extend_from_slice(&[0, 0x13, 0x01, 0]);
    hello.extend(list((room - hello.len()).min(65_537), 2, 4, |n| {
        let kind = u16::try_from(0x8000 + n).expect("fits").to_be_bytes();
        vec![kind[0], kind[1], 0, 0]
    }));
    [("certificates", certificates), ("names", message(13, &request)), ("extensions", message(2, &hello))]
}

#[test]
fn hostile_messages_sent_first_are_priced() {
    // The records of one record's message, and of the longest rustls reads.
    for (records, size) in [(client::MAX_RECORD, 16_384), (4 * client::MAX_RECORD, 65_539)] {
        let limits = Limits { records, ..SMALL };
        for (what, message) in hostile_messages(size) {
            let measured = hostile(limits, &message);
            // Decoded whole, then refused: a hello of TLS 1.2 is expected.
            assert_eq!(measured.failed, Some(Error::Protocol), "{what}");
            if what != "extensions" {
                let decoded = 18 * u64::try_from(message.len()).expect("fits");
                assert!(past(&measured) > i64::try_from(decoded).expect("fits"), "{what}: {measured:?}");
            }
        }
    }
}

#[test]
fn a_hostile_chain_of_empty_certificates_is_priced_and_kept() {
    for versions in [Versions::Tls12, Versions::Tls13] {
        // Empty certificates after the leaf and the intermediate, to fill
        // the records held, and the longest message rustls reads; rustls
        // accepts the chain, and keeps it.
        for (records, empty) in [(client::MAX_RECORD, 15_000), (4 * client::MAX_RECORD, 64_000)] {
            let per = if versions == Versions::Tls12 { 3 } else { 5 };
            let mut case = Case::plain(Limits { records, ..LARGE }, 1_000, 1_000);
            case.server.chain = Chain::Padded(empty / per);
            case.server.versions = versions;
            let measured = measure(&case, 7);
            assert_eq!(measured.failed, None, "{case:?}");
            if versions == Versions::Tls12 && records == 4 * client::MAX_RECORD {
                assert!(10 * measured.most >= 9 * measured.bound, "the bound is tight: {measured:?}");
            }
        }
    }
}
