//! Real native TLS over actual IO/kernel conversations with a rustls peer.
//! Ciphertext is observed only when actually emitted; it is not deterministic
//! replay evidence (tls.md, 3.6; testing-strategy.md, 2.4 and 6).

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};

use skein_io::kernel::{Complete, Op, Submit};
use skein_io::{Event as IoEvent, Io, Limits as IoLimits, Request as IoRequest};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token};
use skein_sim::{Config, Sim};
use skein_tls::client::{self, Limits, Version, native};

use crate::{pki, server::Server};

const LISTENER: Token = Token::new(1);
const CLIENT: Token = Token::new(2);
const SERVER: Token = Token::new(3);
const LIMITS: Limits = Limits { read: 16, send: 16, records: client::MAX_RECORD };

/// Actual capacity/lifecycle supplement; all old classic worlds remain intact
/// (tls.md, 3.6; testing-strategy.md, 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Case {
    /// Late output while both actual reads await the peer.
    Late,
    /// Real one-byte plaintext record retained in flight leaves full next
    /// native envelope one byte short, with one queue slot free.
    ByteOneShort,
    /// Exact full byte envelope grants beside that same actual flight.
    ByteExact,
    /// One actual flight plus the original one queued body fills Send slots;
    /// bytes still hold the full next envelope exactly.
    SlotsFull,
    /// Real IO Close after `ClientHello` with pre-Ready upper Wanted; no lower
    /// output request remains to emit Cancelled before physical Closed.
    DirectClosed,
    /// Actual blocked lower right is cancelled by physical IO Close; local
    /// native Close is delivered before actual lower Closed.
    LocalCloseFirst,
    /// The same genuine lifecycle has physical Closed before queued native
    /// Close, which must be inert and emit no second `Client::Closed`.
    PhysicalCloseFirst,
}

/// Facts from an actually settled IO/TLS conversation, not predicted outcomes
/// or ciphertext replay (tls.md, 3.6; testing-strategy.md, 6).
#[derive(Debug)]
pub struct Outcome {
    /// Exact authenticated peer plaintext from actual rustls processing.
    pub server_plaintext: Vec<u8>,
    /// Actual authenticated response met the original upper read.
    pub reply: Vec<u8>,
    /// Actual native plaintext terminals, each with its issued identity.
    pub terminals: Vec<(Token, OutputOutcome)>,
    /// At least one real grant arrived while upper and ciphertext read rights
    /// both remained unanswered; native Room did not satisfy either read.
    pub behind_both_reads: bool,
    /// Actual byte/slot backpressure witnessed before a retained kernel Send
    /// completion freed its original ownership.
    pub blocked: bool,
    /// Native local Close terminal count, after actual physical Closed.
    pub closed: u32,
    /// Actual unsolicited lower cancellation lifecycle count.
    pub transport_closing: u32,
    /// Actual physical termination count when no local Close won first.
    pub transport_closed: u32,
}

/// Distinct actual resources witnesses in the bounded IO world (tls.md, 3.6).
struct Resources {
    listener: bool,
    client: bool,
    server: bool,
}

/// Distinct actual reads witnesses in the bounded IO world (tls.md, 3.6).
struct Reads {
    upper_read: bool,
    lower_read: bool,
}

/// Distinct actual progress witnesses in the bounded IO world (tls.md, 3.6).
struct Progress {
    ready: bool,
    responded: bool,
    server_closing: bool,
}

/// Distinct actual close witnesses in the bounded IO world (tls.md, 3.6).
struct Close {
    physical_close_queued: bool,
    physical_close_issued: bool,
    native_close_queued: bool,
}

/// Distinct actual evidence witnesses in the bounded IO world (tls.md, 3.6).
struct Evidence {
    blocked: bool,
    behind_both_reads: bool,
}

struct Driver {
    resources: Resources,
    reads: Reads,
    progress: Progress,
    close: Close,
    evidence: Evidence,
    io: Io,
    io_env: Env<IoLimits>,
    io_events: Queue<IoEvent>,
    submissions: Queue<Submit>,
    requests: Queue<IoRequest>,
    completions: Queue<Complete>,
    tls: native::Client,
    tls_env: Env<Limits>,
    above: Queue<native::Event>,
    below: Queue<native::LowerRequest>,
    peer: Server,
    peer_output: VecDeque<u8>,
    peer_read: Option<u32>,
    peer_body: bool,
    peer_right: Option<(Token, u32)>,
    peer_sequence: u64,
    client_socket: Option<Token>,
    client_output: Option<(Token, u32)>,
    server_socket: Option<Token>,
    listener: Option<Token>,
    next_plain: u32,
    pending_upper: Option<Token>,
    case: Case,
    first_record: u32,
    watch_bytes: Option<Box<[u8]>>,
    watch_op: Option<Token>,
    held: Option<Complete>,
    released: bool,
    closed: u32,
    transport_closing: u32,
    transport_closed: u32,
    reply: Vec<u8>,
    terminals: Vec<(Token, OutputOutcome)>,
}

impl Driver {
    fn new(case: Case, version: Version) -> Self {
        let envelope = native::largest_room(&LIMITS).expect("native compatible envelope");
        let first_record: u32 = match version {
            Version::Tls12 => 30,
            Version::Tls13 => 23,
        };
        let output = match case {
            Case::Late | Case::DirectClosed => envelope,
            Case::ByteOneShort | Case::LocalCloseFirst | Case::PhysicalCloseFirst => envelope + first_record - 1,
            Case::ByteExact => envelope + first_record,
            Case::SlotsFull => envelope + 2 * first_record,
        };
        let io_limits = IoLimits {
            sockets: 3,
            refusals: 1,
            intake: client::LARGEST_READ,
            receive: 1024,
            output,
            sends: 1,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        };
        let server = pki::Server {
            versions: match version {
                Version::Tls12 => pki::Versions::Tls12,
                Version::Tls13 => pki::Versions::Tls13,
            },
            ..pki::Server::plain()
        };
        let mut requests = Queue::with_capacity(64);
        requests.push(IoRequest::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        Self {
            evidence: Evidence { blocked: false, behind_both_reads: false },
            close: Close { physical_close_queued: false, physical_close_issued: false, native_close_queued: false },
            progress: Progress { ready: false, responded: false, server_closing: false },
            reads: Reads { upper_read: false, lower_read: false },
            resources: Resources { listener: false, client: false, server: false },
            io: Io::new(&io_limits),
            io_env: Env { now: Time::ZERO, wall: pki::VALID, limits: io_limits },
            io_events: Queue::with_capacity(64),
            submissions: Queue::with_capacity(64),
            requests,
            completions: Queue::with_capacity(64),
            tls: native::Client::new(
                &pki::client(&[]),
                pki::name(),
                &LIMITS,
                &native::LowerLimits { read: io_limits.intake, output: io_limits.output, sends: io_limits.sends },
            )
            .expect("compatible actual IO cap"),
            tls_env: Env { now: Time::ZERO, wall: pki::VALID, limits: LIMITS },
            above: Queue::with_capacity(native::UP_MAX_OUT.above),
            below: Queue::with_capacity(native::UP_MAX_OUT.below),
            peer: Server::new(server.config()),
            peer_output: VecDeque::new(),
            peer_read: None,
            peer_body: false,
            peer_right: None,
            peer_sequence: 0,
            client_socket: None,
            client_output: None,
            server_socket: None,
            listener: None,
            next_plain: 0,
            pending_upper: None,
            case,
            first_record,
            watch_bytes: None,
            watch_op: None,
            held: None,
            released: false,
            closed: 0,
            transport_closing: 0,
            transport_closed: 0,
            reply: Vec::new(),
            terminals: Vec::new(),
        }
    }

    fn tls_down(&mut self, request: native::Request) {
        assert!(self.above.is_empty() && self.below.is_empty());
        native::down(&mut self.tls, &self.tls_env, request, &mut self.above, &mut self.below);
        self.route(native::DOWN_MAX_OUT);
    }

    fn tls_up(&mut self, event: native::LowerEvent) {
        assert!(self.above.is_empty() && self.below.is_empty());
        native::up(&mut self.tls, &self.tls_env, event, &mut self.above, &mut self.below);
        self.route(native::UP_MAX_OUT);
    }

    fn route(&mut self, maximum: skein_tls::MaxOut) {
        assert!(self.above.len() <= maximum.above && self.below.len() <= maximum.below);
        while let Some(request) = self.below.pop() {
            let socket = self.client_socket.expect("actual client endpoint");
            match request {
                native::LowerRequest::Stream(down) => self.requests.push(IoRequest::Stream { stream: socket, down }),
                native::LowerRequest::Output(down) => {
                    if let OutputDown::Send { bytes, .. } = &down {
                        if self.case == Case::SlotsFull && self.progress.ready && self.next_plain == 2 {
                            assert_eq!(
                                bytes.len(),
                                usize::try_from(self.first_record).expect("finite"),
                                "second actually queued one-byte ciphertext body uses the same full ownership"
                            );
                        }
                        if self.progress.ready
                            && self.next_plain == 1
                            && self.case != Case::Late
                            && self.watch_bytes.is_none()
                            && self.watch_op.is_none()
                            && !self.close.native_close_queued
                        {
                            assert_eq!(
                                bytes.len(),
                                usize::try_from(self.first_record).expect("actual record length"),
                                "real one-byte encrypted record, no assumed owed control"
                            );
                            assert!(self.watch_bytes.replace(bytes.clone()).is_none());
                        }
                    }
                    let direct_close = self.case == Case::DirectClosed && matches!(&down, OutputDown::Send { .. });
                    self.requests.push(IoRequest::Output { stream: socket, down });
                    if direct_close {
                        self.queue_physical_close();
                    }
                }
            }
        }
        while let Some(event) = self.above.pop() {
            self.native_event(event);
        }
    }

    fn ask_plain(&mut self, bytes: u32) {
        let right = Token::new(100 + u64::from(self.next_plain));
        assert!(self.pending_upper.replace(right).is_none());
        self.tls_down(native::Request::Output(OutputDown::Room { right, bytes }));
    }

    fn native_event(&mut self, event: native::Event) {
        match event {
            native::Event::Client(client::Event::Ready(_)) => {
                assert!(!self.progress.ready && self.case != Case::DirectClosed);
                self.progress.ready = true;
                self.reads.upper_read = true;
                self.tls_down(native::Request::Client(client::Request::Stream(Down::Demand {
                    read: Read::Fill(2),
                    room: 0,
                })));
                self.ask_plain(if self.case == Case::Late { LIMITS.send } else { 1 });
            }
            native::Event::Output(OutputUp::Settled { right, outcome }) => self.plain_terminal(right, outcome),
            native::Event::Client(client::Event::Stream(Up::Bytes(bytes))) => {
                assert!(self.reads.upper_read && self.progress.ready);
                self.reads.upper_read = false;
                self.reply.extend_from_slice(&bytes);
                assert_eq!(self.reply, b"ok");
                self.close.native_close_queued = true;
                self.tls_down(native::Request::Client(client::Request::Close));
                self.queue_physical_close();
            }
            native::Event::Client(client::Event::Closed) => {
                assert!(self.resources.client, "native local terminal follows actual physical Closed");
                self.closed += 1;
                self.close_server();
            }
            native::Event::TransportClosing => {
                self.transport_closing += 1;
                match self.case {
                    Case::LocalCloseFirst => {
                        self.close.native_close_queued = true;
                        self.tls_down(native::Request::Client(client::Request::Close));
                    }
                    Case::PhysicalCloseFirst => self.close.native_close_queued = true,
                    Case::Late | Case::ByteOneShort | Case::ByteExact | Case::SlotsFull | Case::DirectClosed => {
                        panic!("unexpected physical cancellation")
                    }
                }
            }
            native::Event::TransportClosed => {
                assert!(self.resources.client, "actual physical lifecycle");
                self.transport_closed += 1;
                if self.close.native_close_queued || self.case == Case::DirectClosed {
                    self.tls_down(native::Request::Client(client::Request::Close));
                }
                self.close_server();
            }
            native::Event::Client(client::Event::Stream(Up::End)) => {
                assert!(self.reply == b"ok", "authenticated clean End only after reply");
            }
            native::Event::Client(client::Event::Stream(Up::Room)) => {
                panic!("native output cannot answer classic read")
            }
            native::Event::Client(client::Event::Stream(Up::Failed(fault))) => {
                panic!("positive native TLS stream failed: {fault:?}")
            }
            native::Event::Client(client::Event::Failed(error)) => panic!("positive real TLS failed: {error:?}"),
        }
    }

    fn plain_terminal(&mut self, right: Token, outcome: OutputOutcome) {
        assert_eq!(self.pending_upper.take(), Some(right), "actual exact upper right");
        self.terminals.push((right, outcome));
        match outcome {
            OutputOutcome::Granted => {
                assert!(
                    self.reads.upper_read && self.reads.lower_read && self.reply.is_empty(),
                    "both actual reads still unanswered when plaintext grant wins"
                );
                self.evidence.behind_both_reads = true;
                self.next_plain += 1;
                let plain: &[u8] = match self.case {
                    Case::Late => b"ping",
                    Case::ByteOneShort | Case::ByteExact | Case::LocalCloseFirst | Case::PhysicalCloseFirst => {
                        if self.next_plain == 1 { b"p" } else { b"ing" }
                    }
                    Case::SlotsFull => match self.next_plain {
                        1 => b"p",
                        2 => b"i",
                        3 => b"ng",
                        _ => panic!("bounded plaintext parts"),
                    },
                    Case::DirectClosed => panic!("pre-Ready output cannot be granted"),
                };
                self.tls_down(native::Request::Output(OutputDown::Send { right, bytes: plain.into() }));
                let again = match self.case {
                    Case::Late | Case::DirectClosed => false,
                    Case::SlotsFull => self.next_plain < 3,
                    Case::ByteOneShort | Case::ByteExact | Case::LocalCloseFirst | Case::PhysicalCloseFirst => {
                        self.next_plain < 2
                    }
                };
                if again {
                    self.ask_plain(if self.case == Case::SlotsFull && self.next_plain == 1 { 1 } else { LIMITS.send });
                }
            }
            OutputOutcome::Cancelled => assert!(
                matches!(self.case, Case::DirectClosed | Case::LocalCloseFirst | Case::PhysicalCloseFirst),
                "only actual lifecycle modes cancel pending upper"
            ),
            OutputOutcome::Failed(fault) => panic!("positive native output failed: {fault:?}"),
        }
    }

    fn queue_physical_close(&mut self) {
        if !self.close.physical_close_queued {
            self.close.physical_close_queued = true;
            self.requests.push(IoRequest::Close { entity: self.client_socket.expect("actual client resource") });
        }
    }

    fn close_server(&mut self) {
        if !self.progress.server_closing {
            self.progress.server_closing = true;
            self.requests.push(IoRequest::Close { entity: self.server_socket.expect("actual server resource") });
        }
    }

    fn peer_read(&mut self, bytes: u32) {
        assert!(self.peer_read.replace(bytes).is_none());
        self.requests.push(IoRequest::Stream {
            stream: self.server_socket.expect("bound server"),
            down: Down::Demand { read: Read::Fill(bytes), room: 0 },
        });
    }

    fn peer_room(&mut self) {
        if self.peer_right.is_none() && !self.peer_output.is_empty() && !self.progress.server_closing {
            let bytes = u32::try_from(self.peer_output.len())
                .expect("bounded fixture server flight")
                .min(self.io_env.limits.output);
            let right = Token::new(self.peer_sequence);
            self.peer_sequence = self.peer_sequence.checked_add(1).expect("bounded server rights");
            self.peer_right = Some((right, bytes));
            self.requests.push(IoRequest::Output {
                stream: self.server_socket.expect("bound server"),
                down: OutputDown::Room { right, bytes },
            });
        }
    }

    fn peer_bytes(&mut self, bytes: &[u8]) {
        assert_eq!(
            bytes.len(),
            usize::try_from(self.peer_read.take().expect("real server Fill")).expect("bounded read")
        );
        let next = if self.peer_body { client::HEADER } else { u32::from(u16::from_be_bytes([bytes[3], bytes[4]])) };
        self.peer_body = !self.peer_body;
        self.peer.receive(bytes);
        assert!(
            self.peer.failed.is_none(),
            "actual server authenticates native client's ciphertext: {:?}",
            self.peer.failed
        );
        if self.peer.received == b"ping" && !self.progress.responded {
            self.progress.responded = true;
            self.peer.write(b"ok");
        }
        self.peer_output.extend(self.peer.transmit());
        self.peer_room();
        if !self.progress.server_closing {
            self.peer_read(next);
        }
    }

    fn io_event(&mut self, event: IoEvent) {
        match event {
            IoEvent::Listening { owner, listener, addr } => {
                assert_eq!(owner, LISTENER);
                self.listener = Some(listener);
                self.requests.push(IoRequest::Connect { owner: CLIENT, addr });
            }
            IoEvent::Connecting { owner, socket } => {
                assert_eq!(owner, CLIENT);
                self.client_socket = Some(socket);
            }
            IoEvent::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER);
                self.server_socket = Some(socket);
                self.requests.push(IoRequest::Bind { socket, owner: SERVER });
                self.peer_read(client::HEADER);
                self.requests.push(IoRequest::Close { entity: self.listener.expect("actual listener") });
            }
            IoEvent::Connected { owner } => {
                assert_eq!(owner, CLIENT);
                self.tls_down(native::Request::Client(client::Request::Handshake));
                if self.case == Case::DirectClosed {
                    self.ask_plain(1);
                }
            }
            IoEvent::Stream { owner, up } => {
                if owner == CLIENT {
                    match &up {
                        Up::Bytes(_) | Up::Failed(_) => self.reads.lower_read = false,
                        Up::End | Up::Room => {}
                    }
                    self.tls_up(native::LowerEvent::Stream(up));
                } else {
                    assert_eq!(owner, SERVER);
                    match up {
                        Up::Bytes(bytes) if !self.progress.server_closing => self.peer_bytes(&bytes),
                        Up::Bytes(_) | Up::End | Up::Failed(_) => {
                            assert!(self.progress.server_closing || self.close.physical_close_queued);
                        }
                        Up::Room => panic!("real server IO uses native output"),
                    }
                }
            }
            IoEvent::Output { owner, up } => {
                if owner == CLIENT {
                    let OutputUp::Settled { right, .. } = &up;
                    let (expected, _) = self.client_output.take().expect("actual issued client IO output right");
                    assert_eq!(*right, expected, "actual IO terminal settles its exact issued reservation");
                    self.tls_up(native::LowerEvent::Output(up));
                } else {
                    assert_eq!(owner, SERVER);
                    let OutputUp::Settled { right, outcome } = up;
                    let (expected, cap) = self.peer_right.take().expect("actual pending server IO output");
                    assert_eq!(right, expected);
                    match outcome {
                        OutputOutcome::Granted => {
                            let bytes: Vec<u8> = self
                                .peer_output
                                .drain(..usize::try_from(cap).expect("bounded flight prefix"))
                                .collect();
                            self.requests.push(IoRequest::Output {
                                stream: self.server_socket.expect("server resource"),
                                down: OutputDown::Send { right, bytes: bytes.into() },
                            });
                            self.peer_room();
                        }
                        OutputOutcome::Cancelled | OutputOutcome::Failed(_) => assert!(self.progress.server_closing),
                    }
                }
            }
            IoEvent::Closed { owner } => {
                if owner == CLIENT {
                    self.resources.client = true;
                    self.reads.lower_read = false;
                    self.tls_up(native::LowerEvent::Closed);
                } else if owner == SERVER {
                    self.resources.server = true;
                } else {
                    assert_eq!(owner, LISTENER);
                    self.resources.listener = true;
                }
            }
            IoEvent::Failed { owner, error } => {
                assert!(
                    owner == SERVER && self.progress.server_closing,
                    "positive actual IO owner failed: {owner:?} {error:?}"
                );
            }
            IoEvent::Spawned { .. } | IoEvent::Exited { .. } | IoEvent::Usage { .. } | IoEvent::Shutdown { .. } => {
                panic!("TLS socket world owns no process or service signals")
            }
        }
    }

    fn observe_kernel_sends(&mut self) {
        // Correlate the real emitted ciphertext box with its actual kernel Send
        // token, then retain only that actual completion to make ownership full.
        if let Some(watch) = &self.watch_bytes {
            let count = self.submissions.len();
            for _ in 0..count {
                let submission = self.submissions.pop().expect("bounded actual submissions");
                if let Op::Send { bytes, .. } = &submission.kind
                    && bytes.as_ref() == watch.as_ref()
                {
                    assert!(self.watch_op.replace(submission.op).is_none(), "one actual ciphertext Send");
                }
                self.submissions.push(submission);
            }
        }
        if self.watch_op.is_some() {
            self.watch_bytes = None;
        }
    }

    fn release_pressure(&mut self) {
        if self.evidence.blocked
            && self.close.physical_close_issued
            && matches!(self.case, Case::LocalCloseFirst | Case::PhysicalCloseFirst)
            && self.held.is_some()
        {
            self.released = true;
            self.completions.push(self.held.take().expect("actual held winner after real IO Close admission"));
            return;
        }
        if self.held.is_some() {
            let target = match self.case {
                Case::SlotsFull => 2,
                Case::Late
                | Case::ByteOneShort
                | Case::ByteExact
                | Case::DirectClosed
                | Case::LocalCloseFirst
                | Case::PhysicalCloseFirst => 1,
            };
            if self.next_plain == target
                && self.pending_upper.is_some()
                && self.client_output.is_some()
                && !self.io.is_ready()
            {
                let (_, asked) = self.client_output.expect("actual IO has consumed the pending ciphertext Room");
                assert_eq!(asked, native::room_for(LIMITS.send).expect("checked exact full envelope"));
                match self.case {
                    Case::ByteOneShort | Case::SlotsFull | Case::LocalCloseFirst | Case::PhysicalCloseFirst => {
                        self.evidence.blocked = true;
                        if matches!(self.case, Case::LocalCloseFirst | Case::PhysicalCloseFirst) {
                            self.queue_physical_close();
                        }
                    }
                    Case::Late | Case::ByteExact | Case::DirectClosed => {}
                }
                if !matches!(self.case, Case::LocalCloseFirst | Case::PhysicalCloseFirst)
                    || self.close.physical_close_issued
                {
                    self.released = true;
                    self.completions.push(self.held.take().expect("actual held winner released"));
                }
            } else if self.case == Case::ByteExact && self.next_plain == 2 {
                self.released = true;
                self.completions.push(self.held.take().expect("exact-credit grant wins beside real flight"));
            }
        }
    }

    fn completion(&mut self, completion: Complete) {
        if !self.released && self.watch_op == Some(completion.op) {
            assert!(self.held.replace(completion).is_none(), "one actual retained kernel winner");
            return;
        }
        let events = self.io_events.len();
        let submissions = self.submissions.len();
        skein_io::up(&mut self.io, &self.io_env, completion, &mut self.io_events, &mut self.submissions);
        self.check_io(events, submissions, skein_io::MAX_OUT_UP);
    }

    fn io_room(&self, maximum: skein_io::MaxOut) -> bool {
        self.io_events.room() >= maximum.events && self.submissions.room() >= maximum.submissions
    }

    fn check_io(&self, events: u32, submissions: u32, maximum: skein_io::MaxOut) {
        assert!(self.io_events.len() - events <= maximum.events, "actual IO event maximum");
        assert!(self.submissions.len() - submissions <= maximum.submissions, "actual IO kernel maximum");
    }

    fn iterate(&mut self) {
        while self.io.is_ready() && self.io_room(skein_io::MAX_OUT_RESUME) {
            let events = self.io_events.len();
            let submissions = self.submissions.len();
            skein_io::resume(&mut self.io, &self.io_env, &mut self.io_events, &mut self.submissions);
            self.check_io(events, submissions, skein_io::MAX_OUT_RESUME);
        }
        while self.io_room(skein_io::MAX_OUT_UP) {
            let Some(completion) = self.completions.pop() else { break };
            self.completion(completion);
        }
        while self.io.is_due(self.io_env.now) && self.io_room(skein_io::MAX_OUT_FIRE) {
            let events = self.io_events.len();
            let submissions = self.submissions.len();
            skein_io::fire(&mut self.io, &self.io_env, &mut self.io_events, &mut self.submissions);
            self.check_io(events, submissions, skein_io::MAX_OUT_FIRE);
        }
        while let Some(event) = self.io_events.pop() {
            self.io_event(event);
        }
        while self.io.takes() && self.io_room(skein_io::MAX_OUT_DOWN) {
            let Some(request) = self.requests.pop() else { break };
            if let IoRequest::Stream { stream, down: Down::Demand { read, room: 0 } } = &request
                && Some(*stream) == self.client_socket
            {
                self.reads.lower_read = *read != Read::Nothing;
            }
            if matches!(&request, IoRequest::Close { entity } if Some(*entity) == self.client_socket) {
                self.close.physical_close_issued = true;
            }
            if let IoRequest::Output { stream, down: OutputDown::Room { right, bytes } } = &request
                && Some(*stream) == self.client_socket
            {
                assert!(self.client_output.replace((*right, *bytes)).is_none(), "one actual issued IO output right");
            }
            let events = self.io_events.len();
            let submissions = self.submissions.len();
            skein_io::down(&mut self.io, &self.io_env, request, &mut self.submissions);
            self.check_io(events, submissions, skein_io::MAX_OUT_DOWN);
        }
        self.io.reclaim();
    }
}

/// Run the selected supplement against real IO/simulator sockets and a real
/// rustls server, returning only after every kernel/IO right and descriptor
/// settles. Native TLS ciphertext is not asserted replayable. Driver iterations
/// are bounded at 40,000 (tls.md, 3.6; testing-strategy.md, 6 and 8).
#[must_use]
pub fn conversation(seed: u64, mut config: Config, case: Case, version: Version) -> Outcome {
    config.wall = pki::VALID;
    let mut sim = Sim::new(seed, config);
    let pid = sim.spawn_process();
    let mut driver = Driver::new(case, version);
    for _ in 0..40_000_u32 {
        driver.io_env.now = sim.now();
        driver.tls_env.now = sim.now();
        sim.reap(pid, &mut driver.completions);
        driver.iterate();
        driver.observe_kernel_sends();
        sim.submit(pid, &mut driver.submissions);
        driver.release_pressure();
        if driver.resources.client && driver.resources.server && driver.resources.listener && driver.io.is_empty() {
            assert!(driver.requests.is_empty() && driver.completions.is_empty() && driver.held.is_none());
            sim.assert_quiescent(pid);
            sim.assert_no_open_fds(pid);
            let outcome = Outcome {
                server_plaintext: driver.peer.received,
                reply: driver.reply,
                terminals: driver.terminals,
                behind_both_reads: driver.evidence.behind_both_reads,
                blocked: driver.evidence.blocked,
                closed: driver.closed,
                transport_closing: driver.transport_closing,
                transport_closed: driver.transport_closed,
            };
            return outcome;
        }
        let busy = driver.io.is_ready()
            || !driver.requests.is_empty()
            || !driver.completions.is_empty()
            || sim.deferred(pid)
            || sim.ready(pid) > 0;
        if !busy {
            let next = match sim.next_due() {
                Some(kernel) => Some(driver.io.next_deadline().map_or(kernel, |io| kernel.min(io))),
                None => driver.io.next_deadline(),
            };
            sim.advance_to(next.expect("unsettled actual TLS world retains real work"));
        }
    }
    panic!("bounded actual IO/TLS conversation settles: {}", sim.render_trace());
}
