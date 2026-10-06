//! Shared framing over actual skein-io socket machines and skein-sim kernel
//! operations. Native bytes/queued Send slots are real IO admission limits;
//! no adapter invents Granted from classic Room or write completion.
use crate::machine::schema;
use skein_channel::{
    Disposition, Event as ChannelEvent, FrameWriter, LowerEvent, LowerRequest, Machine, Opening, OpeningMode,
    Request as ChannelRequest, Role, Step,
};
use skein_io::kernel::{Complete, Submit};
use skein_io::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Request};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Config, Entry, Sim};
use std::net::{Ipv4Addr, SocketAddr};

const LISTENER: Token = Token::new(1);
const CLIENT: Token = Token::new(2);
const SERVER: Token = Token::new(3);

/// Replay evidence only from actual simulated kernel/IO and decoded payloads.
#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Real submitted/completed operations, with short transfers and scheduling.
    pub trace: Vec<Entry>,
    /// Exact known service bodies delivered, owner/kind/bytes in actual order.
    pub bodies: Vec<(Token, u16, Vec<u8>)>,
    /// Actual native output terminal count; includes eager debt replenishment.
    pub terminals: u32,
    /// Whether a whole-cap actual grant progressed beside a live frame read.
    pub grant_beside_read: bool,
    /// Bounded iterations to all actual resource and named-right retirement.
    pub iterations: u32,
}

struct Peer {
    socket: Option<Token>,
    channel: Machine,
    input_live: bool,
    received: bool,
    closed: bool,
}

impl Peer {
    fn new(role: Role) -> Peer {
        Peer {
            socket: None,
            channel: Machine::new(schema(role, OpeningMode::AcceptHighest, true, 1)),
            input_live: false,
            received: false,
            closed: false,
        }
    }
}

struct Driver {
    io: Io,
    env: Env<Limits>,
    subs: Queue<Submit>,
    events: Queue<Event>,
    requests: Queue<Request>,
    completions: Queue<Complete>,
    client: Peer,
    server: Peer,
    channel_events: Queue<ChannelEvent>,
    channel_lower: Queue<LowerRequest>,
    listener_closed: bool,
    listener: Option<Token>,
    bodies: Vec<(Token, u16, Vec<u8>)>,
    terminals: u32,
    grant_beside_read: bool,
}

impl Driver {
    fn new() -> Driver {
        let limits = Limits {
            sockets: 3,
            refusals: 1,
            intake: 32,
            receive: 11,
            output: 1024,
            sends: 1,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        };
        let mut driver = Driver {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            subs: Queue::with_capacity(64),
            events: Queue::with_capacity(64),
            requests: Queue::with_capacity(64),
            completions: Queue::with_capacity(64),
            client: Peer::new(Role::Initiator),
            server: Peer::new(Role::Responder),
            channel_events: Queue::with_capacity(2),
            channel_lower: Queue::with_capacity(3),
            listener_closed: false,
            listener: None,
            bodies: Vec::new(),
            terminals: 0,
            grant_beside_read: false,
        };
        driver.requests.push(Request::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        driver
    }

    fn peer(&mut self, owner: Token) -> &mut Peer {
        if owner == CLIENT {
            &mut self.client
        } else {
            assert_eq!(owner, SERVER);
            &mut self.server
        }
    }

    fn channel_poll(&mut self, owner: Token) {
        let peer = if owner == CLIENT { &mut self.client } else { &mut self.server };
        skein_channel::poll(&mut peer.channel, &mut self.channel_events, &mut self.channel_lower);
    }

    fn channel_request(&mut self, owner: Token, request: ChannelRequest) -> Step {
        let peer = if owner == CLIENT { &mut self.client } else { &mut self.server };
        skein_channel::down(&mut peer.channel, request, &mut self.channel_events, &mut self.channel_lower)
    }

    fn route_lower(&mut self, owner: Token) {
        while let Some(request) = self.channel_lower.pop() {
            let peer = self.peer(owner);
            let socket = peer.socket.expect("actual connected/bound IO stream");
            match request {
                LowerRequest::Stream(down) => {
                    match &down {
                        skein_lib::stream::Down::Demand { read, room } => {
                            assert_eq!(*room, 0);
                            peer.input_live = *read != skein_lib::stream::Read::Nothing;
                        }
                        skein_lib::stream::Down::Send(_) => panic!("channel must use native Send"),
                        skein_lib::stream::Down::Finish => assert!(!peer.input_live),
                    }
                    self.requests.push(Request::Stream { stream: socket, down });
                }
                LowerRequest::Output(down) => self.requests.push(Request::Output { stream: socket, down }),
            }
        }
    }

    fn send_final(&mut self, owner: Token, kind: u16, bytes: &[u8]) {
        let peer = self.peer(owner);
        let mut writer = FrameWriter::new(
            peer.channel.schema(),
            1,
            kind,
            u32::try_from(bytes.len()).expect("literal bounded payload"),
        )
        .expect("source schema send kind");
        writer.put(bytes).expect("exact measured body");
        let encoded = writer.finish().expect("one exact final allocation");
        assert_eq!(
            self.channel_request(owner, ChannelRequest::Send { owner: Token::new(80), encoded: Some(encoded) }),
            Step::NeedPoll
        );
        assert!(matches!(self.channel_events.pop(), Some(ChannelEvent::Sent { .. })));
        assert_eq!(self.channel_request(owner, ChannelRequest::Finish), Step::NeedPoll);
        self.channel_poll(owner);
    }

    fn channel_upper(&mut self, owner: Token) {
        while let Some(event) = self.channel_events.pop() {
            match event {
                ChannelEvent::Ready { version } => {
                    assert_eq!(version, 1);
                    if owner == CLIENT {
                        let peer = self.peer(owner);
                        let mut writer = FrameWriter::new(peer.channel.schema(), 1, 257, 8).expect("first source kind");
                        writer.put(b"question").expect("measured request");
                        let encoded = writer.finish().expect("exact frame");
                        assert_eq!(
                            self.channel_request(
                                owner,
                                ChannelRequest::Send { owner: Token::new(70), encoded: Some(encoded) }
                            ),
                            Step::NeedPoll
                        );
                        assert!(matches!(self.channel_events.pop(), Some(ChannelEvent::Sent { .. })));
                    }
                }
                ChannelEvent::Body { receipt, version, kind, bytes } => {
                    assert_eq!(version, 1);
                    assert!(!self.peer(owner).received, "one paused service body");
                    let expected = if owner == CLIENT { b"answer".as_slice() } else { b"question".as_slice() };
                    assert_eq!(bytes.as_ref(), expected, "independent literal service oracle");
                    assert_eq!(kind, if owner == CLIENT { 385 } else { 257 });
                    self.bodies.push((owner, kind, bytes.to_vec()));
                    self.peer(owner).received = true;
                    drop(bytes);
                    let peer = if owner == CLIENT { &mut self.client } else { &mut self.server };
                    assert_eq!(
                        skein_channel::resolve(
                            &mut peer.channel,
                            receipt,
                            Disposition::DecodedPause,
                            &mut self.channel_events,
                            &mut self.channel_lower
                        ),
                        Step::NeedPoll
                    );
                    if owner == SERVER {
                        self.send_final(owner, 385, b"answer");
                    } else {
                        assert_eq!(self.channel_request(owner, ChannelRequest::Finish), Step::NeedPoll);
                        self.channel_poll(owner);
                    }
                }
                ChannelEvent::Sent { .. } => {}
                ChannelEvent::Closed { .. } => {}
                ChannelEvent::Opening { .. }
                | ChannelEvent::Unsupported { .. }
                | ChannelEvent::Refused { .. }
                | ChannelEvent::Unsent { .. }
                | ChannelEvent::ReadEnded => panic!("unexpected upper control in positive IO framing scene"),
            }
        }
    }

    fn channel_event(&mut self, owner: Token, event: LowerEvent) {
        let needs_decode = matches!(&event, LowerEvent::Stream(skein_lib::stream::Up::Bytes(_)));
        if needs_decode {
            self.peer(owner).input_live = false;
        }
        if let LowerEvent::Output(skein_lib::stream::OutputUp::Settled { .. }) = &event {
            self.terminals = self.terminals.checked_add(1).expect("bounded actual terminals");
            if self.peer(owner).input_live {
                self.grant_beside_read = true;
            }
        }
        let peer = if owner == CLIENT { &mut self.client } else { &mut self.server };
        let step = skein_channel::up(&mut peer.channel, event, &mut self.channel_events, &mut self.channel_lower);
        // Body handling resolves and does its one final poll. All other paths
        // finish upper synchronous admission first, then exactly one poll.
        self.channel_upper(owner);
        if step == Step::NeedPoll {
            self.channel_poll(owner);
            self.channel_upper(owner);
        }
        self.route_lower(owner);
    }

    fn on(&mut self, event: Event) {
        match event {
            Event::Listening { owner, listener, addr } => {
                assert_eq!(owner, LISTENER);
                self.listener = Some(listener);
                self.requests.push(Request::Connect { owner: CLIENT, addr });
            }
            Event::Connecting { owner, socket } => {
                assert_eq!(owner, CLIENT);
                self.client.socket = Some(socket);
            }
            Event::Connected { owner } => {
                assert_eq!(owner, CLIENT);
                assert_eq!(
                    self.channel_request(
                        owner,
                        ChannelRequest::Open {
                            owner: Token::new(60),
                            opening: Opening {
                                channel: 1,
                                lowest: 1,
                                highest: 1,
                                name: Box::from([]),
                                secret: Box::from([])
                            }
                        }
                    ),
                    Step::NeedPoll
                );
                assert!(matches!(self.channel_events.pop(), Some(ChannelEvent::Sent { .. })));
                self.channel_poll(owner);
                self.route_lower(owner);
            }
            Event::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER);
                self.server.socket = Some(socket);
                self.requests.push(Request::Bind { socket, owner: SERVER });
                self.requests.push(Request::Close { entity: self.listener.expect("actual listener") });
                self.channel_poll(SERVER);
                self.route_lower(SERVER);
            }
            Event::Output { owner, up } => self.channel_event(owner, LowerEvent::Output(up)),
            Event::Stream { owner, up } => {
                let end = matches!(&up, skein_lib::stream::Up::End);
                self.channel_event(owner, LowerEvent::Stream(up));
                if end {
                    assert!(self.peer(owner).received);
                    let socket = self.peer(owner).socket.expect("actual peer");
                    self.requests.push(Request::Close { entity: socket });
                }
            }
            Event::Closed { owner } => {
                if owner == LISTENER {
                    assert!(!self.listener_closed);
                    self.listener_closed = true;
                } else {
                    self.channel_event(owner, LowerEvent::Closed);
                    assert!(self.peer(owner).channel.is_retired(), "actual IO terminal precedes resource Closed");
                    self.peer(owner).closed = true;
                }
            }
            Event::Failed { owner, error } => panic!("actual IO scene failed {owner:?} {error:?}"),
            Event::Spawned { .. } | Event::Exited { .. } => panic!("socket framing scene has no process"),
        }
    }

    fn room(&self, max: MaxOut) -> bool {
        self.events.room() >= max.events && self.subs.room() >= max.submissions
    }

    fn iterate(&mut self) {
        while self.io.is_ready() && self.room(MAX_OUT_RESUME) {
            skein_io::resume(&mut self.io, &self.env, &mut self.events, &mut self.subs);
        }
        if !self.io.is_ready() {
            while self.room(MAX_OUT_UP) {
                let Some(complete) = self.completions.pop() else { break };
                skein_io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.subs);
            }
        }
        while self.io.is_due(self.env.now) && self.room(MAX_OUT_FIRE) {
            skein_io::fire(&mut self.io, &self.env, &mut self.events, &mut self.subs);
        }
        while let Some(event) = self.events.pop() {
            self.on(event);
        }
        while self.io.takes() && self.room(MAX_OUT_DOWN) {
            let Some(request) = self.requests.pop() else { break };
            skein_io::down(&mut self.io, &self.env, request, &mut self.subs);
        }
        self.io.reclaim();
    }
}

/// Run actual framing+IO+sim until both named rights and resource lifetimes
/// settle, with deterministic replay and real independent grant/read evidence.
#[must_use]
pub fn conversation(seed: u64, config: Config) -> Outcome {
    let mut sim = Sim::new(seed, config);
    let pid = sim.spawn_process();
    let mut driver = Driver::new();
    for iterations in 1..=20_000 {
        driver.env.now = sim.now();
        driver.env.wall = sim.wall();
        sim.reap(pid, &mut driver.completions);
        driver.iterate();
        sim.submit(pid, &mut driver.subs);
        if driver.client.closed && driver.server.closed && driver.listener_closed && driver.io.is_empty() {
            assert!(driver.requests.is_empty() && driver.completions.is_empty());
            sim.assert_quiescent(pid);
            sim.assert_no_open_fds(pid);
            assert_eq!(driver.bodies.len(), 2);
            assert!(driver.grant_beside_read);
            return Outcome {
                trace: sim.trace().to_vec(),
                bodies: driver.bodies,
                terminals: driver.terminals,
                grant_beside_read: driver.grant_beside_read,
                iterations,
            };
        }
        let busy = driver.io.is_ready()
            || !driver.requests.is_empty()
            || !driver.completions.is_empty()
            || sim.deferred(pid)
            || sim.ready(pid) > 0;
        if !busy {
            let next = match sim.next_due() {
                Some(kernel) => match driver.io.next_deadline() {
                    Some(io) => Some(kernel.min(io)),
                    None => Some(kernel),
                },
                None => driver.io.next_deadline(),
            };
            sim.advance_to(next.expect("unsettled framing scene retains actual operation/deadline"));
        }
    }
    panic!("native framing did not settle: {}", sim.render_trace());
}
