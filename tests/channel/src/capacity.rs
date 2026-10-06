//! Real IO capacity witnesses (channel.md §§4–6; io.md §§3.3–3.4).
//!
//! This world owns a real channel machine, actual IO socket/output entities,
//! bounded64-record routing scratch, real Sim operations and held owning
//! completions/terminals. `pressure` runs a bounded deterministic scene through
//! ordinary IO entrances and records actual requests, events and kernel reaps.
//! No service body decoding, fabricated grants or private IO state is involved.
//! Machine C=1024/body64/32/chunk7 and native N=1 are fixed; native byte caps
//! C, C+72, C+71 or checked3C isolate byte versus queued-slot admission. Each
//! delayed real winner is routed once and all actual resources retire (§§4–6).
use crate::machine::{RESPONDER_TERMS, literal, schema};
use skein_channel::{
    Event as ChannelEvent, FrameWriter, LowerEvent, LowerRequest, Machine, Opening, OpeningMode,
    Request as ChannelRequest, Role, Step,
};
use skein_io::kernel::{Complete, Done, Fd, Op, Submit};
use skein_io::{Event, Io, Limits, MAX_OUT_DOWN, MAX_OUT_FIRE, MAX_OUT_RESUME, MAX_OUT_UP, MaxOut, Request};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall, stream};
use skein_sim::{Config, Entry, Sim, Summary};
use std::net::{Ipv4Addr, SocketAddr};

const LISTENER: Token = Token::new(1);
const CLIENT: Token = Token::new(2);
const PEER: Token = Token::new(3);
const PEER_RIGHT: Token = Token::new(900);
const C: u32 = 1024;

/// Which independent native admission dimension must block (channel.md §4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Pressure {
    /// Actual output cap C; any retained owning frame prevents whole-C Room (channel.md §§4,6).
    Bytes,
    /// Actual output cap C+f; the same real retained frame leaves exact C (channel.md §§4,6).
    BytesExact,
    /// Actual output cap C+f-1; the same retained frame leaves C-1 (channel.md §§4,6).
    BytesOneShort,
    /// Actual output cap 3C; a real flight plus N=1 queued blocks only the slot (channel.md §§4,6).
    Slots,
}

/// Independent observations of actual requests/events/completion routing (§4).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Observation {
    /// Actual kernel operation reaped from Sim; routing can be delayed (channel.md §§4,6).
    Reaped {
        /// Actual kernel operation identity, echoed once by the backend (channel.md §§4,6).
        operation: Token,
        /// Real operation shape, including owning Send length and offset (channel.md §§4,6).
        summary: Summary,
        /// Actual backend result; never invented by the world (channel.md §§4,6).
        result: Result<Done, skein_io::kernel::Error>,
    },
    /// Actual native whole-cap request emitted by the shared machine (channel.md §§4,6).
    Room {
        /// Current caller-generated identity, eventually matched once (channel.md §§4,6).
        right: Token,
    },
    /// Actual native frame admitted by IO, with whole-box capacity ownership (channel.md §§4,6).
    Send {
        /// Actual affine native right consumed exactly once (channel.md §§4,6).
        right: Token,
        /// Independently read wire kind, 257 then optionally258 (channel.md §§4,6).
        kind: u16,
        /// Actual complete frame length retained by IO until fully retired (channel.md §§4,6).
        bytes: u32,
    },
    /// Actual IO Granted terminal, optionally held before core delivery (channel.md §§4,6).
    Granted {
        /// Exact admitted Room identity, never a test-generated grant (channel.md §§4,6).
        right: Token,
        /// Whether the real terminal is deliberately held across logical Close (channel.md §§4,6).
        held: bool,
    },
    /// Real final Send completion forwarded to IO; full box then retires (channel.md §§4,6).
    Retired {
        /// Actual complete owning frame kind, not a write-completion ACK above (channel.md §§4,6).
        kind: u16,
    },
    /// Real old/wrong output token sent down to IO; no terminal follows it (channel.md §§4,6).
    Stale {
        /// Already consumed native right; cannot spend the current right (channel.md §§4,6).
        right: Token,
    },
    /// Actual core read-only demand or withdrawal, independent of output (§4).
    Demand {
        /// Actual bounded read shape; never a world-manufactured answer (channel.md §§4,6).
        read: stream::Read,
    },
    /// Actual named right cancellation request after logical stop (§6).
    Cancel {
        /// Exact pending identity; an already emitted winner still must retire (channel.md §§4,6).
        right: Token,
    },
    /// Actual affine Release request, consuming a retained real grant (§4,6).
    Release {
        /// Exact real granted identity, spent once without sending a frame (channel.md §§4,6).
        right: Token,
    },
    /// Actual lower output Finish after read withdrawal and right retirement (§6).
    Finish,
    /// Immediate core closure observed while actual native winner is retained (§6).
    LogicalClosed,
    /// Actual physical client resource Closed after release/cancel settlement (§6).
    ResourceClosed,
    /// Core observes the one matching real grant and updates its C/P/S ledger (channel.md §§4,6).
    CoreGrant {
        /// Exact actual native winner delivered after negative controls (channel.md §§4,6).
        right: Token,
        /// Conservative core debt before real delivery (channel.md §§4,6).
        before: u32,
        /// Conservative core debt after delivery; zero only in live state (channel.md §§4,6).
        after: u32,
    },
}

/// Settled real IO evidence with positive and negative capacity observations (channel.md §§4,6).
#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Actual Sim kernel trace, including real fragmented writes and closes (channel.md §§4,6).
    pub trace: Vec<Entry>,
    /// Actual native requests/terminals and real reaped completion chronology (channel.md §§4,6).
    pub observations: Vec<Observation>,
    /// Retained whole-box admission bytes at the capacity-control entrance (channel.md §§4,6).
    pub retained_bytes: u32,
    /// Actual IO byte room at that entrance, independently accounted (channel.md §§4,6).
    pub free_bytes: u32,
    /// Submitted bytes whose real winning completion still awaits IO routing (channel.md §§4,6).
    pub unretired_write_bytes: u32,
    /// Actual flight plus queued owning frames at the whole-C Room entrance (channel.md §§4,6).
    pub retained_frames: u32,
    /// Actual matching native winners counted once, for the pressured right (channel.md §§4,6).
    pub matching_grants: u32,
    /// The independent client read remained live throughout output pressure (channel.md §§4,6).
    pub read_live: bool,
    /// Actual late winner was Released after logical Close, when requested (channel.md §§4,6).
    pub late_winner_retired: bool,
}

struct Driver {
    io: Io,
    env: Env<Limits>,
    submissions: Queue<Submit>,
    completions: Queue<Complete>,
    io_events: Queue<Event>,
    requests: Queue<Request>,
    channel_events: Queue<ChannelEvent>,
    channel_lower: Queue<LowerRequest>,
    channel: Machine,
    client: Option<Token>,
    peer: Option<Token>,
    listener: Option<Token>,
    client_fd: Option<Fd>,
    pending: Option<Token>,
    granted: Option<Token>,
    consumed: Option<Token>,
    held_write: Option<Complete>,
    held_grant: Option<stream::OutputUp>,
    retired_data: [bool; 2],
    ready: bool,
    read_live: bool,
    closing: bool,
    closed_entities: [bool; 3],
    admitted: Vec<(u16, u32)>,
    owned_frames: u32,
    observations: Vec<Observation>,
    pressure: Pressure,
    target: Option<Token>,
    matching_grants: u32,
}

impl Driver {
    fn new(pressure: Pressure) -> Driver {
        let output = match pressure {
            Pressure::Bytes => C,
            Pressure::BytesExact => C.checked_add(72).expect("exact C beside retained frame"),
            Pressure::BytesOneShort => C.checked_add(72).expect("same whole frame").checked_sub(1).expect("one short"),
            Pressure::Slots => C.checked_mul(3).expect("actual byte headroom"),
        };
        let limits = Limits {
            sockets: 3,
            refusals: 1,
            intake: 32,
            receive: 11,
            output,
            sends: 1,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        };
        let mut driver = Driver {
            io: Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            submissions: Queue::with_capacity(64),
            completions: Queue::with_capacity(64),
            io_events: Queue::with_capacity(64),
            requests: Queue::with_capacity(64),
            channel_events: Queue::with_capacity(2),
            channel_lower: Queue::with_capacity(3),
            channel: Machine::new(schema(Role::Initiator, OpeningMode::AskParent, true, 1)),
            client: None,
            peer: None,
            listener: None,
            client_fd: None,
            pending: None,
            granted: None,
            consumed: None,
            held_write: None,
            held_grant: None,
            retired_data: [false; 2],
            ready: false,
            read_live: false,
            closing: false,
            closed_entities: [false; 3],
            admitted: Vec::new(),
            owned_frames: 0,
            observations: Vec::new(),
            pressure,
            target: None,
            matching_grants: 0,
        };
        driver.requests.push(Request::Listen { owner: LISTENER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        driver
    }

    fn queue_channel(&mut self, request: ChannelRequest) {
        let step = skein_channel::down(&mut self.channel, request, &mut self.channel_events, &mut self.channel_lower);
        if step == Step::NeedPoll {
            skein_channel::poll(&mut self.channel, &mut self.channel_events, &mut self.channel_lower);
        }
        self.upper();
        self.lower();
    }

    fn send_frame(&mut self, kind: u16, length: u32) {
        let mut writer =
            FrameWriter::new(self.channel.schema(), 1, kind, length).expect("original source maximum sender rule");
        let bytes = vec![9; usize::try_from(length).expect("bounded typed body")];
        writer.put(&bytes).expect("exact measured source maximum");
        let encoded = writer.finish().expect("one direct final frame allocation");
        self.queue_channel(ChannelRequest::Send { owner: Token::new(u64::from(kind)), encoded: Some(encoded) });
    }

    fn lower(&mut self) {
        while let Some(request) = self.channel_lower.pop() {
            let socket = self.client.expect("actual connected client socket");
            match request {
                LowerRequest::Stream(down) => {
                    match &down {
                        stream::Down::Demand { read, room } => {
                            assert_eq!(*room, 0);
                            self.read_live = *read != stream::Read::Nothing;
                            self.observations.push(Observation::Demand { read: *read });
                        }
                        stream::Down::Finish => {
                            assert!(!self.read_live);
                            self.observations.push(Observation::Finish);
                        }
                        stream::Down::Send(_) => panic!("no classic adapter in native capacity world"),
                    }
                    self.requests.push(Request::Stream { stream: socket, down });
                }
                LowerRequest::Output(down) => {
                    match &down {
                        stream::OutputDown::Room { right, bytes } => {
                            assert_eq!(*bytes, C);
                            assert!(self.pending.replace(*right).is_none());
                            assert!(self.granted.is_none());
                            self.observations.push(Observation::Room { right: *right });
                            let required = if self.pressure == Pressure::Slots { 2 } else { 1 };
                            if self.admitted.len() >= required && self.target.is_none() {
                                self.target = Some(*right);
                            }
                        }
                        stream::OutputDown::Send { right, bytes } => {
                            assert_eq!(self.granted.take(), Some(*right));
                            self.consumed = Some(*right);
                            self.owned_frames += 1;
                            let kind = u16::from_be_bytes([bytes[0], bytes[1]]);
                            if kind > 17 {
                                let length = u32::try_from(bytes.len()).expect("actual owning frame");
                                self.admitted.push((kind, length));
                                self.observations.push(Observation::Send { right: *right, kind, bytes: length });
                            }
                        }
                        stream::OutputDown::Release { right } => {
                            assert_eq!(self.granted.take(), Some(*right));
                            self.observations.push(Observation::Release { right: *right });
                        }
                        stream::OutputDown::Cancel { right } => {
                            assert_eq!(self.pending, Some(*right));
                            self.observations.push(Observation::Cancel { right: *right });
                        }
                    }
                    self.requests.push(Request::Output { stream: socket, down });
                }
            }
        }
    }

    fn upper(&mut self) {
        while let Some(event) = self.channel_events.pop() {
            match event {
                ChannelEvent::Ready { version } => {
                    assert_eq!(version, 1);
                    self.ready = true;
                }
                ChannelEvent::Sent { .. } => {}
                ChannelEvent::Closed { .. } => {
                    assert!(self.closing);
                    self.observations.push(Observation::LogicalClosed);
                }
                ChannelEvent::Opening { .. }
                | ChannelEvent::Body { .. }
                | ChannelEvent::Unsupported { .. }
                | ChannelEvent::Refused { .. }
                | ChannelEvent::Unsent { .. }
                | ChannelEvent::ReadEnded => panic!("unexpected capacity-world upper event"),
            }
        }
    }

    fn core_event(&mut self, event: LowerEvent) {
        let step = skein_channel::up(&mut self.channel, event, &mut self.channel_events, &mut self.channel_lower);
        assert!(!matches!(step, Step::NeedDecode { .. }), "peer only sends common opening controls");
        if step == Step::NeedPoll {
            skein_channel::poll(&mut self.channel, &mut self.channel_events, &mut self.channel_lower);
        }
        self.upper();
        self.lower();
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
                self.client = Some(socket);
            }
            Event::Connected { owner } => {
                assert_eq!(owner, CLIENT);
                self.queue_channel(ChannelRequest::Open {
                    owner: Token::new(80),
                    opening: Opening { channel: 1, lowest: 1, highest: 1, name: Box::from([]), secret: Box::from([]) },
                });
            }
            Event::Accepted { owner, socket, .. } => {
                assert_eq!(owner, LISTENER);
                self.peer = Some(socket);
                self.requests.push(Request::Bind { socket, owner: PEER });
                self.requests.push(Request::Close { entity: self.listener.expect("actual listener") });
                self.requests.push(Request::Stream {
                    stream: socket,
                    down: stream::Down::Demand { read: stream::Read::Fill(1), room: 0 },
                });
                self.requests.push(Request::Output {
                    stream: socket,
                    down: stream::OutputDown::Room { right: PEER_RIGHT, bytes: C },
                });
            }
            Event::Output { owner, up: stream::OutputUp::Settled { right, outcome } } => {
                self.output_event(owner, right, outcome);
            }
            Event::Stream { owner, up } => self.stream_event(owner, up),
            Event::Closed { owner } => self.closed_event(owner),
            Event::Failed { owner, error } => panic!("positive actual IO capacity failed {owner:?} {error:?}"),
            Event::Spawned { .. } | Event::Exited { .. } => panic!("socket capacity world has no process"),
        }
    }

    fn output_event(&mut self, owner: Token, right: Token, outcome: stream::OutputOutcome) {
        if owner == PEER {
            assert_eq!(right, PEER_RIGHT);
            assert_eq!(outcome, stream::OutputOutcome::Granted);
            let mut controls = literal(2, &[0, 1]).into_vec();
            controls.extend_from_slice(&literal(16, RESPONDER_TERMS));
            self.requests.push(Request::Output {
                stream: self.peer.expect("bound peer"),
                down: stream::OutputDown::Send { right, bytes: controls.into_boxed_slice() },
            });
        } else {
            assert_eq!(owner, CLIENT);
            if outcome == stream::OutputOutcome::Granted {
                assert!(self.granted.replace(right).is_none());
                let held = self.target == Some(right);
                self.observations.push(Observation::Granted { right, held });
                if held {
                    self.matching_grants += 1;
                    assert!(self.held_grant.replace(stream::OutputUp::Settled { right, outcome }).is_none());
                    return;
                }
            }
            assert_eq!(self.pending.take(), Some(right));
            self.core_event(LowerEvent::Output(stream::OutputUp::Settled { right, outcome }));
        }
    }

    fn stream_event(&mut self, owner: Token, up: stream::Up) {
        if owner == CLIENT {
            if matches!(&up, stream::Up::Bytes(_)) {
                self.read_live = false;
            }
            self.core_event(LowerEvent::Stream(up));
        } else {
            assert_eq!(owner, PEER);
            match up {
                stream::Up::Bytes(bytes) => {
                    assert_eq!(bytes.len(), 1);
                    self.requests.push(Request::Stream {
                        stream: self.peer.expect("bound peer"),
                        down: stream::Down::Demand { read: stream::Read::Fill(1), room: 0 },
                    });
                }
                stream::Up::End => {
                    self.requests.push(Request::Close { entity: self.peer.expect("actual peer") });
                }
                stream::Up::Failed(fault) => panic!("positive peer failed {fault:?}"),
                stream::Up::Room => panic!("peer output is genuinely native"),
            }
        }
    }

    fn closed_event(&mut self, owner: Token) {
        if owner == LISTENER {
            assert!(!self.closed_entities[2]);
            self.closed_entities[2] = true;
        } else if owner == CLIENT {
            assert!(!self.closed_entities[0]);
            self.closed_entities[0] = true;
            self.observations.push(Observation::ResourceClosed);
            self.core_event(LowerEvent::Closed);
            assert!(self.channel.is_retired());
        } else {
            assert_eq!(owner, PEER);
            assert!(!self.closed_entities[1]);
            self.closed_entities[1] = true;
        }
    }

    fn room(&self, maximum: MaxOut) -> bool {
        self.io_events.room() >= maximum.events && self.submissions.room() >= maximum.submissions
    }

    fn complete(&mut self, completion: Complete) {
        if let Op::Send { fd, bytes, from } = &completion.kind
            && Some(*fd) == self.client_fd
        {
            let kind = u16::from_be_bytes([bytes[0], bytes[1]]);
            if let Ok(Done::Count(count)) = &completion.result {
                let next = from.checked_add(*count).expect("actual valid write count");
                if next == u32::try_from(bytes.len()).expect("actual owning frame length") {
                    self.owned_frames = self.owned_frames.checked_sub(1).expect("actual whole IO box retired");
                    if kind > 17 {
                        if kind == 257 {
                            self.retired_data[0] = true;
                        } else {
                            assert_eq!(kind, 258);
                            self.retired_data[1] = true;
                        }
                        self.observations.push(Observation::Retired { kind });
                    }
                }
            }
        }
        skein_io::up(&mut self.io, &self.env, completion, &mut self.io_events, &mut self.submissions);
    }

    fn iterate(&mut self) {
        while self.io.is_ready() && self.room(MAX_OUT_RESUME) {
            skein_io::resume(&mut self.io, &self.env, &mut self.io_events, &mut self.submissions);
        }
        if !self.io.is_ready() {
            while self.room(MAX_OUT_UP) {
                let Some(completion) = self.completions.pop() else { break };
                self.complete(completion);
            }
        }
        while self.io.is_due(self.env.now) && self.room(MAX_OUT_FIRE) {
            skein_io::fire(&mut self.io, &self.env, &mut self.io_events, &mut self.submissions);
        }
        while let Some(event) = self.io_events.pop() {
            self.on(event);
        }
        while self.io.takes() && self.room(MAX_OUT_DOWN) {
            let Some(request) = self.requests.pop() else { break };
            skein_io::down(&mut self.io, &self.env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }

    fn stale(&mut self) {
        let right = self.consumed.expect("real consumed old grant");
        let socket = self.client.expect("actual client");
        self.observations.push(Observation::Stale { right });
        for down in [
            stream::OutputDown::Cancel { right },
            stream::OutputDown::Release { right },
            stream::OutputDown::Send { right, bytes: Box::from([]) },
        ] {
            self.requests.push(Request::Output { stream: socket, down });
        }
    }

    fn retained(&self) -> (u32, u32) {
        let mut bytes = 0_u32;
        let mut frames = 0_u32;
        for (kind, length) in &self.admitted {
            if (*kind == 257 && !self.retired_data[0]) || (*kind == 258 && !self.retired_data[1]) {
                bytes = bytes.checked_add(*length).expect("real bounded IO owned output");
                frames += 1;
            }
        }
        (bytes, frames)
    }
}

struct World {
    sim: Sim,
    pid: skein_sim::Pid,
    driver: Driver,
    reaped: Queue<Complete>,
    hold_first: bool,
}

impl World {
    fn new(seed: u64, pressure: Pressure) -> World {
        let mut sim = Sim::new(seed, Config { buffer: 1, ..Config::calm() });
        let pid = sim.spawn_process();
        World { sim, pid, driver: Driver::new(pressure), reaped: Queue::with_capacity(64), hold_first: false }
    }

    fn cycle(&mut self) {
        self.driver.env.now = self.sim.now();
        self.driver.env.wall = self.sim.wall();
        self.sim.reap(self.pid, &mut self.reaped);
        while let Some(completion) = self.reaped.pop() {
            self.driver.observations.push(Observation::Reaped {
                operation: completion.op,
                summary: Summary::of(&completion.kind),
                result: completion.result.clone(),
            });
            let hold = if let Op::Send { fd, bytes, from } = &completion.kind {
                if Some(*fd) == self.driver.client_fd
                    && bytes.starts_with(&[1, 1])
                    && self.hold_first
                    && completion.result == Ok(Done::Count(1))
                {
                    match self.driver.pressure {
                        Pressure::Bytes | Pressure::BytesExact | Pressure::BytesOneShort => {
                            from.checked_add(1) == u32::try_from(bytes.len()).ok()
                        }
                        Pressure::Slots => *from == 0,
                    }
                } else {
                    false
                }
            } else {
                false
            };
            if hold {
                assert!(self.driver.held_write.replace(completion).is_none());
                self.hold_first = false;
            } else {
                self.driver.completions.push(completion);
            }
        }
        self.driver.iterate();
        for submission in &self.driver.submissions {
            if let Op::Connect { fd, .. } = &submission.kind {
                self.driver.client_fd = Some(*fd);
            }
        }
        self.sim.submit(self.pid, &mut self.driver.submissions);
        let busy = self.driver.io.is_ready()
            || !self.driver.requests.is_empty()
            || !self.driver.completions.is_empty()
            || self.sim.deferred(self.pid)
            || self.sim.ready(self.pid) > 0;
        if !busy {
            let next = match self.sim.next_due() {
                Some(kernel) => match self.driver.io.next_deadline() {
                    Some(io) => Some(kernel.min(io)),
                    None => Some(kernel),
                },
                None => self.driver.io.next_deadline(),
            };
            if let Some(next) = next {
                self.sim.advance_to(next);
            }
        }
    }

    fn until_ready(&mut self) {
        for _ in 0..10_000 {
            self.cycle();
            if self.driver.ready
                && self.driver.granted.is_some()
                && self.driver.channel.queued_bytes() == 0
                && self.driver.owned_frames == 0
            {
                return;
            }
        }
        panic!("actual opening did not settle {}", self.sim.render_trace());
    }

    fn until_hold(&mut self) {
        for _ in 0..10_000 {
            self.cycle();
            if self.driver.held_write.is_some() {
                return;
            }
        }
        panic!("actual framed write hold was not reached {}", self.sim.render_trace());
    }

    fn release_write(&mut self) {
        let completion = self.driver.held_write.take().expect("held actual reaped completion");
        self.driver.completions.push(completion);
    }

    fn until_grant(&mut self) {
        for _ in 0..10_000 {
            self.cycle();
            if self.driver.held_grant.is_some() && self.driver.retired_data[0] {
                return;
            }
        }
        panic!("exact real capacity did not grant {}", self.sim.render_trace());
    }

    fn boundary(&mut self, pressure: Pressure) -> (u32, u32, u32, u32, bool, u32) {
        let target = self.driver.target.expect("actual pressured whole-C Room");
        assert_eq!(self.driver.pending, Some(target));
        let (retained_bytes, retained_frames) = self.driver.retained();
        let free_bytes =
            self.driver.env.limits.output.checked_sub(retained_bytes).expect("actual full-box capacity ledger");
        let held = self.driver.held_write.as_ref().expect("real retained framed Send completion");
        let Op::Send { bytes, from, .. } = &held.kind else { panic!("held operation must be genuine Send") };
        assert_eq!(held.result, Ok(Done::Count(1)), "actual positive final fragment winner");
        let unretired_write_bytes =
            u32::try_from(bytes.len()).expect("real frame").checked_sub(*from).expect("actual offset");
        let live_read = self.driver.read_live;
        assert!(live_read, "read-only input remains genuinely live");
        assert_eq!(self.driver.channel.pending_bytes(), 0, "all framed output moved into real IO");
        assert_eq!(
            self.driver.channel.queued_bytes(),
            self.driver.admitted.last().expect("data admitted").1,
            "core debt cannot reset without matching real grant"
        );
        assert!(!self.driver.channel.is_ready(), "pending native right alone cannot spin");
        match pressure {
            Pressure::Bytes => {
                assert_eq!(retained_frames, 1);
                assert_eq!(unretired_write_bytes, 1);
                assert_eq!(free_bytes, C.checked_sub(72).expect("retained full72 box"));
            }
            Pressure::BytesExact => {
                assert_eq!(retained_frames, 1);
                assert_eq!(unretired_write_bytes, 1);
                assert_eq!(free_bytes, C, "exact whole-C admission despite real framed flight");
                assert_eq!(self.driver.matching_grants, 1, "actual IO Granted while frame is still retained");
                assert!(self.driver.held_grant.is_some());
            }
            Pressure::BytesOneShort => {
                assert_eq!(retained_frames, 1);
                assert_eq!(unretired_write_bytes, 1);
                assert_eq!(free_bytes, C.checked_sub(1).expect("one byte short"));
            }
            Pressure::Slots => {
                assert_eq!(retained_frames, 2, "one real flight plus N=1 queued");
                assert!(free_bytes >= C, "slot pressure independently excludes byte shortage");
            }
        }
        let before = self.driver.channel.queued_bytes();
        self.driver.stale();
        for _ in 0..8 {
            self.cycle();
        }
        assert_eq!(self.driver.pending, Some(target));
        assert!(self.driver.requests.is_empty(), "every observed native request really crossed IO down");
        assert!(self.driver.completions.is_empty(), "only the explicit owning winner remains held");
        assert_eq!(self.sim.ready(self.pid), 0, "no unhandled backend progress replaces the capacity boundary");
        assert_eq!(self.driver.channel.queued_bytes(), before);
        assert!(!self.driver.channel.is_ready());
        assert!(!self.driver.io.is_ready(), "actual unavailable or held output grant has no runnable IO work");
        if pressure != Pressure::BytesExact {
            assert!(self.driver.held_grant.is_none(), "actual pressured Room has no Granted while capacity is short");
            assert_eq!(self.driver.matching_grants, 0, "no early winning native terminal");
        } else {
            assert_eq!(self.driver.matching_grants, 1, "wrong old token cannot consume genuine current grant");
        }
        (retained_bytes, free_bytes, unretired_write_bytes, retained_frames, live_read, before)
    }

    fn close(&mut self) {
        self.driver.closing = true;
        self.driver.queue_channel(ChannelRequest::Close);
    }

    fn deliver_grant(&mut self) {
        let grant = self.driver.held_grant.take().expect("actual emitted winning native terminal");
        let stream::OutputUp::Settled { right, .. } = grant;
        let before = self.driver.channel.queued_bytes();
        assert_eq!(self.driver.pending.take(), Some(right));
        self.driver.core_event(LowerEvent::Output(grant));
        let after = self.driver.channel.queued_bytes();
        self.driver.observations.push(Observation::CoreGrant { right, before, after });
        if self.driver.closing {
            assert!(self.driver.granted.is_none(), "real late grant Released during retirement");
        } else {
            assert_eq!(after, 0, "only actual matching live Granted clears S");
        }
    }

    fn settle(&mut self) {
        self.driver.requests.push(Request::Close { entity: self.driver.client.expect("actual client") });
        for _ in 0..20_000 {
            self.cycle();
            if self.driver.closed_entities[0]
                && self.driver.closed_entities[1]
                && self.driver.closed_entities[2]
                && self.driver.io.is_empty()
            {
                self.sim.assert_quiescent(self.pid);
                self.sim.assert_no_open_fds(self.pid);
                assert!(self.driver.channel.is_retired());
                return;
            }
        }
        panic!("capacity native lifetimes did not settle {}", self.sim.render_trace());
    }
}

/// Exercise genuine IO byte/slot boundaries, with wrong-token/no-spin controls (channel.md §4; lifecycle §6).
/// and optional real queued Granted winner delivered after logical Close (§4,6).
/// Native caps C, C+f, C+f-1 and 3C isolate full-box, exact, one-short and (channel.md §4; lifecycle §6).
/// slot pressure respectively; f is the same genuine maximum local frame72 (channel.md §§4,6).
/// Native sends stays N=1 beside its flight. Actual IO charges the entire box (channel.md §4; lifecycle §6).
/// until its real final completion is routed, even after Sim wrote that byte (channel.md §§4,6).
#[must_use]
pub fn pressure(seed: u64, pressure: Pressure, late_winner: bool) -> Outcome {
    let mut world = World::new(seed, pressure);
    world.until_ready();
    world.hold_first = true;
    world.driver.send_frame(257, 64);
    world.until_hold();
    if pressure == Pressure::Slots {
        for _ in 0..10_000 {
            if world.driver.granted.is_some() {
                break;
            }
            world.cycle();
        }
        assert!(world.driver.granted.is_some(), "real byte headroom permits second whole-cap grant beside flight");
        world.driver.send_frame(258, 32);
    }
    for _ in 0..8 {
        world.cycle();
    }
    let (retained_bytes, free_bytes, unretired_write_bytes, retained_frames, live_read, before) =
        world.boundary(pressure);
    world.release_write();
    world.until_grant();
    assert_eq!(world.driver.matching_grants, 1, "exact byte+slot availability emits one actual winning terminal");
    assert_eq!(
        world.driver.channel.queued_bytes(),
        before,
        "queued real Granted does not reset S before core delivery"
    );
    assert!(world.driver.retired_data[0], "first full owning frame genuinely retired through IO");
    if pressure == Pressure::Slots {
        assert!(!world.driver.retired_data[1], "queued frame promoted and still genuinely in flight when Room granted");
    }
    if late_winner {
        world.close();
        for _ in 0..2 {
            world.cycle();
        }
    }
    world.deliver_grant();
    if !late_winner {
        world.close();
    }
    world.settle();
    Outcome {
        trace: world.sim.trace().to_vec(),
        observations: world.driver.observations,
        retained_bytes,
        free_bytes,
        unretired_write_bytes,
        retained_frames,
        matching_grants: world.driver.matching_grants,
        read_live: live_read,
        late_winner_retired: late_winner,
    }
}
