//! A complete oversized-for-one-rustls-read HTTP upload through the hosted
//! fake TLS face (fake-llm.md, section 3; testing-strategy.md, section 4.1).

use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_llm_domain::{self as domain, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_fake_peers::{Transport, llm};
use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_tls::client::{self, Client, Event, Request};
use skein_tls_world::pki;
use skein_world::{Host, Memory, Referee, World};

const ROOM: u32 = 64;
const OWNER: Token = Token::new(1);
const TLS: client::Limits = client::Limits { read: 8192, send: 8192, records: client::MAX_RECORD };
const IO: io::Limits = io::Limits {
    sockets: 2,
    refusals: 1,
    intake: 32_768,
    receive: 4096,
    output: 32_768,
    sends: 4,
    accepts: 1,
    backlog: 1,
    close_timeout: Duration::from_secs(1),
    retry: Duration::from_millis(1),
};

#[derive(Clone, Copy)]
enum Upload {
    Connecting,
    Head,
    Body,
    Reading,
    Closing,
}

struct Sender {
    io: io::Io,
    tls: Client,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    above: Queue<Event>,
    below: Queue<Down>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    address: Option<SocketAddr>,
    socket: Option<Token>,
    upload: Upload,
    head: Box<[u8]>,
    body: Box<[u8]>,
}

impl Sender {
    fn new(body: Box<[u8]>) -> Self {
        let target = skein_llm::Endpoint::codex().target;
        let head = [
            b"POST ".as_slice(), target.as_ref(),
            b" HTTP/1.1\r\nHost: skein.test\r\nAuthorization: Bearer token\r\nChatGPT-Account-Id: acc\r\nContent-Type: application/json\r\nContent-Length: 6359\r\n\r\n",
        ].concat().into_boxed_slice();
        Self {
            io: io::Io::new(&IO),
            tls: Client::new(&pki::client(&[]), pki::name(), &TLS),
            events: Queue::with_capacity(ROOM),
            requests: Queue::with_capacity(ROOM),
            above: Queue::with_capacity(ROOM),
            below: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            address: None,
            socket: None,
            upload: Upload::Connecting,
            head,
            body,
        }
    }

    fn down(&mut self, now: Time, wall: Wall, request: Request) {
        client::down(&mut self.tls, &Env { now, wall, limits: TLS }, request, &mut self.above, &mut self.below);
    }

    fn close(&mut self, now: Time) {
        if !matches!(self.upload, Upload::Closing) {
            self.upload = Upload::Closing;
            self.down(now, pki::VALID, Request::Close);
        }
    }
}

impl Host for Sender {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: IO };
        if let Some(address) = self.address.take() {
            self.requests.push(io::Request::Connect { owner: OWNER, addr: address });
        }
        for _ in 0..ROOM {
            if self.io.is_ready() {
                io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
            } else if let Some(complete) = self.completions.pop() {
                io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
            } else if self.io.is_due(now) {
                io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
            } else {
                break;
            }
        }
        for _ in 0..ROOM {
            let Some(event) = self.events.pop() else { break };
            match event {
                io::Event::Connecting { socket, .. } => self.socket = Some(socket),
                io::Event::Connected { .. } => {
                    self.down(now, wall, Request::Handshake);
                }
                io::Event::Stream { up, .. } => {
                    client::up(&mut self.tls, &Env { now, wall, limits: TLS }, up, &mut self.above, &mut self.below);
                }
                io::Event::Closed { .. } => {}
                other @ (io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Output { .. }
                | io::Event::Spawned { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Shutdown { .. }
                | io::Event::Failed { .. }) => panic!("unexpected client io event: {other:?}"),
            }
        }
        for _ in 0..ROOM {
            let Some(event) = self.above.pop() else { break };
            match event {
                Event::Ready(_) => {
                    self.upload = Upload::Head;
                    self.down(
                        now,
                        wall,
                        Request::Stream(Down::Demand {
                            read: Read::Nothing,
                            room: u32::try_from(self.head.len()).expect("head"),
                        }),
                    );
                }
                Event::Stream(Up::Room) => match self.upload {
                    Upload::Head => {
                        self.down(now, wall, Request::Stream(Down::Send(self.head.clone())));
                        self.upload = Upload::Body;
                        self.down(now, wall, Request::Stream(Down::Demand { read: Read::Nothing, room: 6359 }));
                    }
                    Upload::Body => {
                        self.down(now, wall, Request::Stream(Down::Send(self.body.clone())));
                        self.upload = Upload::Reading;
                        self.down(now, wall, Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 }));
                    }
                    Upload::Connecting | Upload::Reading | Upload::Closing => panic!("unexpected room"),
                },
                Event::Stream(Up::Bytes(_)) => {
                    if matches!(self.upload, Upload::Reading) {
                        self.down(now, wall, Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 }));
                    }
                }
                Event::Closed => self.requests.push(io::Request::Close { entity: self.socket.expect("connected") }),
                Event::Stream(Up::End) => {}
                other @ (Event::Stream(Up::Failed(_)) | Event::Failed(_)) => {
                    panic!("unexpected client TLS event: {other:?}")
                }
            }
        }
        for _ in 0..ROOM {
            let Some(down) = self.below.pop() else { break };
            self.requests.push(io::Request::Stream { stream: self.socket.expect("connected"), down });
        }
        for _ in 0..ROOM {
            if !self.io.takes() {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        self.io.is_ready()
            || self.address.is_some()
            || !self.completions.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.above.is_empty()
            || !self.below.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.above.is_empty()
            && self.below.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        0
    }
    fn operations(&self) -> u32 {
        io::operations(&IO).expect("bounded io")
    }
}

enum Process {
    Sender(Box<Sender>),
    Peer(Box<llm::Peer>),
}
impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Sender(sender) => sender.as_ref(),
            Self::Peer(peer) => peer.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Sender(sender) => sender.as_mut(),
            Self::Peer(peer) => peer.as_mut(),
        }
    }
}
impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.host_mut().submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.host().work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.host().next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.host().next_policy_deadline()
    }
    fn is_empty(&self) -> bool {
        self.host().is_empty()
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

struct Judge {
    now: Time,
    text: Box<[u8]>,
    queried: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, now: Time, processes: &mut [Process]) {
        let address = processes.iter().find_map(|process| match process {
            Process::Peer(peer) => peer.address(),
            Process::Sender(_) => None,
        });
        for process in processes {
            match process {
                Process::Sender(sender) => {
                    if sender.socket.is_none() && sender.io.is_empty() && matches!(sender.upload, Upload::Connecting) {
                        sender.address = address;
                    }
                    if self.queried {
                        sender.close(now);
                    }
                }
                Process::Peer(peer) => {
                    if self.queried {
                        peer.shutdown();
                    }
                }
            }
        }
    }
    fn observe(&mut self, now: Time, processes: &[Process]) {
        self.now = now;
        for process in processes {
            if let Process::Peer(peer) = process {
                for observation in peer.observations() {
                    if let llm::Observation::Query { query, .. } = observation {
                        assert_eq!(query.messages.len(), 1);
                        assert_eq!(
                            query.messages[0].parts.as_ref(),
                            [api::Part::Text { text: self.text.clone() }].as_slice()
                        );
                        self.queried = true;
                    }
                }
            }
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        Some(self.now.saturating_add(Duration::from_millis(1)).min(Time::from_nanos(3_000_000_000)))
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(3_000_000_000)).then(|| "TLS body upload did not complete".into())
    }
    fn passed(&self) -> bool {
        self.queried
    }
}

#[test]
fn a_request_body_larger_than_one_tls_read_arrives_whole() {
    let prefix = br#"{"model":"fixture-model","instructions":"body-fixture","stream":true,"store":false,"input":[{"role":"user","content":[{"type":"input_text","text":""#;
    let suffix = br#""}]}],"tools":[],"tool_choice":"auto"}"#;
    let text = vec![b'x'; 6359 - prefix.len() - suffix.len()].into_boxed_slice();
    let body = [prefix.as_slice(), text.as_ref(), suffix.as_slice()].concat().into_boxed_slice();
    assert_eq!(body.len(), 6359);
    let mut config = skein_sim::Config::calm();
    config.wall = pki::VALID;
    config.buffer = 1024;
    let mut world = World::new(27, config, Judge { now: Time::ZERO, text, queried: false }, Memory::Unchecked);
    world.spawn(|| Process::Sender(Box::new(Sender::new(body))));
    world.spawn(|| {
        let limits = skein_llm_world::fake::config();
        let domain = domain::Domain::try_scripted(
            &limits,
            27,
            Box::new([api::Script {
                cue: b"body-fixture".as_slice().into(),
                turns: Box::new([api::Turn { lines: Box::new([]), finish: api::Finish::Stop, tokens: 0 }]),
            }]),
        )
        .expect("bounded script");
        Process::Peer(Box::new(
            llm::Peer::new(
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                Transport::Tls,
                skein_fake_peers::Limits {
                    io: IO,
                    connections: 1,
                    queue: ROOM,
                    plaintext: 32_768,
                    ciphertext: 32_768,
                    observations: 8,
                    observation_bytes: 32_768,
                },
                provider::Config {
                    provider: documents::Provider::OpenAi,
                    path: skein_llm::Endpoint::codex().target,
                    headers: Box::new([]),
                    echo: skein_llm::openai::Echo::NONE,
                    usage_fields: documents::UsageFields::ALL,
                },
                skein_llm::Credential {
                    access_token: b"token".as_slice().into(),
                    account_id: b"acc".as_slice().into(),
                },
                skein_llm_world::fake::limits(&skein_llm_world::limits()),
                domain,
                limits,
            )
            .expect("fake TLS face"),
        ))
    });
    let outcome = world.run();
    assert!(outcome.procs.iter().all(Host::is_empty));
}
