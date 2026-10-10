//! One physical connection and its current call (llm-connection.md, sections
//! 4 and 5). A connection keeps its socket name, optional TLS and LLM machines, and
//! their bounded routing queues. It never knows the owner's domain or retry
//! policy. `route` advances the selected transport and the LLM client.
//!
//! | State | Input | Next | Emits |
//! | --- | --- | --- | --- |
//! | Connecting | connected | Handshaking | TLS handshake |
//! | Handshaking | ready | Calling | LLM request |
//! | Draining | drained, live | Idle | reusable binding |
//! | Draining | drained, owner closing | Ready | take a waiting call or close |
//! | any live | failure or close | Closing | one call terminal, socket close |
//! | Closing | socket closed | Closed | settlement |

#![expect(clippy::single_match, reason = "explicit socket presence cases at the stream boundary")]

use skein_io::Request as IoRequest;
use skein_lib::stream::{Down, Up};
use skein_lib::{Env, Queue, Time, Token};
use skein_llm::client as llm;
use skein_tls::client as tls;

use crate::boundary::{Deadlines, Event};
use crate::deadlines::{Due, Phase as DeadlinePhase, Table};
use crate::limits::Limits;

/// The physical stream's lifecycle, independent of the call's terminal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Phase {
    Connecting,
    Handshaking,
    Head,
    Streaming,
    Draining,
    Idle,
    Ready,
    Closing,
}

/// The owner's close policy and whether its physical abort has started.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Control {
    Live,
    Draining,
    AbortPending,
    Aborting,
}

/// One socket, TLS session and LLM client, named to io by its slab token.
pub(crate) struct Connection {
    pub(crate) endpoint: u32,
    pub(crate) call: Option<Token>,
    pub(crate) reservation: u64,
    pub(crate) socket: Option<Token>,
    pub(crate) phase: Phase,
    pub(crate) tls: Option<tls::Client>,
    pub(crate) llm: llm::Client,
    llm_limits: llm::Limits,
    pub(crate) idle_at: Option<Time>,
    pub(crate) tls_closed: bool,
    pub(crate) socket_close_sent: bool,
    pub(crate) deadlines: Table,
    pub(crate) waiting: bool,
    activity: u64,
    control: Control,
    llm_events: Queue<llm::Event>,
    plain_down: Queue<Down>,
    tls_events: Queue<tls::Event>,
    cipher_down: Queue<Down>,
}

impl Connection {
    pub(crate) fn has_work(&self) -> bool {
        !self.llm_events.is_empty()
            || !self.plain_down.is_empty()
            || !self.tls_events.is_empty()
            || !self.cipher_down.is_empty()
            || (self.calling() && self.llm.has_work())
            || (self.abort_pending())
            || (self.closing() && !self.waiting && self.call.is_none() && self.phase != Phase::Closing)
    }

    pub(crate) fn new(
        endpoint: u32,
        call: Token,
        tls: Option<tls::Client>,
        llm: llm::Client,
        llm_limits: llm::Limits,
        deadlines: Deadlines,
        now: Time,
    ) -> Connection {
        Connection {
            endpoint,
            call: Some(call),
            reservation: 0,
            socket: None,
            phase: Phase::Connecting,
            tls,
            llm,
            llm_limits,
            idle_at: None,
            tls_closed: false,
            socket_close_sent: false,
            deadlines: Table::new(deadlines, now),
            waiting: false,
            activity: 0,
            control: Control::Live,
            llm_events: Queue::with_capacity(64),
            plain_down: Queue::with_capacity(256),
            tls_events: Queue::with_capacity(64),
            cipher_down: Queue::with_capacity(256),
        }
    }

    pub(crate) fn request_close(&mut self, abort: bool) {
        self.control = match self.control {
            Control::Live | Control::Draining => {
                if abort {
                    Control::AbortPending
                } else {
                    Control::Draining
                }
            }
            Control::AbortPending => Control::AbortPending,
            Control::Aborting => Control::Aborting,
        };
    }

    pub(crate) fn aborting(&self) -> bool {
        match self.control {
            Control::Live | Control::Draining => false,
            Control::AbortPending | Control::Aborting => true,
        }
    }

    fn closing(&self) -> bool {
        match self.control {
            Control::Live => false,
            Control::Draining | Control::AbortPending | Control::Aborting => true,
        }
    }

    fn abort_pending(&self) -> bool {
        match self.control {
            Control::AbortPending => true,
            Control::Live | Control::Draining | Control::Aborting => false,
        }
    }

    fn calling(&self) -> bool {
        match self.phase {
            Phase::Head | Phase::Streaming | Phase::Draining => true,
            Phase::Connecting | Phase::Handshaking | Phase::Idle | Phase::Ready | Phase::Closing => false,
        }
    }

    fn terminal(&mut self) {
        match self.phase {
            Phase::Head | Phase::Streaming => self.phase = Phase::Draining,
            Phase::Connecting | Phase::Handshaking | Phase::Draining | Phase::Idle | Phase::Ready | Phase::Closing => {}
        }
    }

    pub(crate) fn sync_deadlines(&mut self, env: &Env<Limits>) {
        let phase = match self.phase {
            Phase::Connecting => DeadlinePhase::Connecting,
            Phase::Handshaking => DeadlinePhase::Handshaking,
            Phase::Head => DeadlinePhase::Head,
            Phase::Streaming => DeadlinePhase::Streaming,
            Phase::Draining => DeadlinePhase::Draining,
            Phase::Idle => DeadlinePhase::Idle,
            Phase::Ready | Phase::Closing => DeadlinePhase::Closing,
        };
        self.deadlines.arm(phase, env.now, env.limits.idle_keep);
    }

    pub(crate) fn connected(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        if self.aborting() {
            self.route(env, owner, up, io);
            return;
        }
        match &mut self.tls {
            Some(client) => {
                self.phase = Phase::Handshaking;
                let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
                tls::down(client, &tls_env, tls::Request::Handshake, &mut self.tls_events, &mut self.cipher_down);
                self.route(env, owner, up, io);
            }
            None => {
                self.phase = Phase::Head;
                self.sync_deadlines(env);
                self.start_call(env, owner, up, io);
            }
        }
    }

    pub(crate) fn start_call(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        llm::down(&mut self.llm, &llm_env, llm::Request::Start, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn next(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        if self.aborting() {
            self.route(env, owner, up, io);
            return;
        }
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        llm::down(&mut self.llm, &llm_env, llm::Request::Next, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn cancel(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        llm::down(&mut self.llm, &llm_env, llm::Request::Cancel, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
        if self.phase == Phase::Connecting {
            self.close_socket(io);
        }
    }

    pub(crate) fn failed(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        if self.aborting() {
            self.route(env, owner, up, io);
            return;
        }
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        llm::abort(
            &mut self.llm,
            &llm_env,
            skein_llm::Failure::Unavailable,
            &mut self.llm_events,
            &mut self.plain_down,
        );
        self.route(env, owner, up, io);
    }

    pub(crate) fn cipher_up(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        event: Up,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        if self.aborting() {
            self.route(env, owner, up, io);
            return;
        }
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        match &mut self.tls {
            Some(client) => tls::up(client, &tls_env, event, &mut self.tls_events, &mut self.cipher_down),
            None => {
                let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
                llm::up(&mut self.llm, &llm_env, event, &mut self.llm_events, &mut self.plain_down);
            }
        }
        self.route(env, owner, up, io);
    }

    pub(crate) fn closed(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        self.socket_close_sent = true;
        if self.abort_pending() {
            self.control = Control::Aborting;
            self.phase = Phase::Closing;
            llm::down(&mut self.llm, &llm_env, llm::Request::Cancel, &mut self.llm_events, &mut self.plain_down);
        }
        llm::closed(&mut self.llm, &llm_env, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
        self.deadlines.arm(DeadlinePhase::Closed, env.now, env.limits.idle_keep);
    }

    pub(crate) fn close_socket(&mut self, io: &mut Queue<IoRequest>) {
        if !self.socket_close_sent {
            match self.socket {
                Some(socket) => {
                    if self.tls_closed && !self.aborting() {
                        io.push(IoRequest::Close { entity: socket });
                    } else {
                        io.push(IoRequest::Abort { entity: socket });
                    }
                    self.socket_close_sent = true;
                }
                None => {}
            }
        }
        self.phase = Phase::Closing;
    }

    fn close_transport(&mut self, env: &Env<Limits>, io: &mut Queue<IoRequest>) {
        match &mut self.tls {
            Some(client) => {
                let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
                tls::down(client, &tls_env, tls::Request::Close, &mut self.tls_events, &mut self.cipher_down);
                self.phase = Phase::Closing;
            }
            None => {
                self.tls_closed = true;
                self.close_socket(io);
            }
        }
    }

    pub(crate) fn timeout(&mut self, env: &Env<Limits>, due: Due, io: &mut Queue<IoRequest>) {
        match self.phase {
            Phase::Draining | Phase::Idle => {
                self.close_transport(env, io);
                self.sync_deadlines(env);
                return;
            }
            Phase::Ready | Phase::Closing => return,
            Phase::Connecting | Phase::Handshaking | Phase::Head | Phase::Streaming => {}
        }
        let phase = match due {
            Due::Call(phase) => phase,
            Due::Keep => unreachable!("keep runs only on an idle connection"),
        };
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        llm::abort(
            &mut self.llm,
            &llm_env,
            skein_llm::Failure::TimedOut { phase },
            &mut self.llm_events,
            &mut self.plain_down,
        );
    }

    #[expect(clippy::too_many_lines, reason = "the bounded routing pass covers both child machines")]
    pub(crate) fn route(&mut self, env: &Env<Limits>, _owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: self.llm_limits };
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        if self.abort_pending() {
            self.control = Control::Aborting;
            llm::down(&mut self.llm, &llm_env, llm::Request::Cancel, &mut self.llm_events, &mut self.plain_down);
            self.phase = Phase::Closing;
            self.socket_close_sent = false;
            self.close_socket(io);
        } else if self.closing() && !self.waiting && self.call.is_none() && self.phase != Phase::Closing {
            llm::down(&mut self.llm, &llm_env, llm::Request::Close, &mut self.llm_events, &mut self.plain_down);
        }
        for _ in 0_u32..64_u32 {
            if let Some(event) = self.llm_events.pop() {
                match event {
                    llm::Event::Delta { owner: call, delta } => up.push(Event::Delta { call, delta }),
                    llm::Event::Block { owner: call, block } => up.push(Event::Block { call, block }),
                    llm::Event::Completed { owner: call, completion } => {
                        up.push(Event::Completed { call, completion });
                        self.call = None;
                        self.terminal();
                        if self.closing() && !self.waiting {
                            llm::down(
                                &mut self.llm,
                                &llm_env,
                                llm::Request::Close,
                                &mut self.llm_events,
                                &mut self.plain_down,
                            );
                        }
                    }
                    llm::Event::Failed { owner: call, failure, evidence, detail } => {
                        up.push(Event::Failed { call, failure, evidence, detail });
                        self.call = None;
                        self.terminal();
                        if self.closing() && !self.waiting {
                            llm::down(
                                &mut self.llm,
                                &llm_env,
                                llm::Request::Close,
                                &mut self.llm_events,
                                &mut self.plain_down,
                            );
                        }
                    }
                    llm::Event::Cancelled { owner: call } => {
                        up.push(Event::Cancelled { call });
                        self.call = None;
                        self.terminal();
                        if self.closing() && !self.waiting {
                            llm::down(
                                &mut self.llm,
                                &llm_env,
                                llm::Request::Close,
                                &mut self.llm_events,
                                &mut self.plain_down,
                            );
                        }
                    }
                    llm::Event::Reusable => {
                        self.phase = if self.closing() { Phase::Ready } else { Phase::Idle };
                        self.idle_at = Some(env.now);
                    }
                    llm::Event::Close => match self.phase {
                        Phase::Handshaking
                        | Phase::Head
                        | Phase::Streaming
                        | Phase::Draining
                        | Phase::Idle
                        | Phase::Ready => {
                            self.close_transport(env, io);
                        }
                        Phase::Connecting => self.close_socket(io),
                        Phase::Closing => {}
                    },
                    llm::Event::Closed => {}
                }
            } else if let Some(down) = self.plain_down.pop() {
                if self.calling() || (self.phase == Phase::Idle || self.phase == Phase::Ready) {
                    match &down {
                        Down::Send(bytes) if !bytes.is_empty() && self.phase == Phase::Head => {
                            self.deadlines.sent(env.now);
                        }
                        Down::Send(_) | Down::Demand { .. } | Down::Finish => {}
                    }
                    match &mut self.tls {
                        Some(client) => tls::down(
                            client,
                            &tls_env,
                            tls::Request::Stream(down),
                            &mut self.tls_events,
                            &mut self.cipher_down,
                        ),
                        None => self.cipher_down.push(down),
                    }
                }
            } else if let Some(event) = self.tls_events.pop() {
                match event {
                    tls::Event::Ready(_) => {
                        if self.aborting() {
                            continue;
                        }
                        self.phase = Phase::Head;
                        self.sync_deadlines(env);
                        llm::down(
                            &mut self.llm,
                            &llm_env,
                            llm::Request::Start,
                            &mut self.llm_events,
                            &mut self.plain_down,
                        );
                    }
                    tls::Event::Stream(plain) => {
                        if self.aborting() {
                            continue;
                        }
                        llm::up(&mut self.llm, &llm_env, plain, &mut self.llm_events, &mut self.plain_down);
                    }
                    tls::Event::Failed(_) => {
                        llm::abort(
                            &mut self.llm,
                            &llm_env,
                            skein_llm::Failure::Unavailable,
                            &mut self.llm_events,
                            &mut self.plain_down,
                        );
                    }
                    tls::Event::Closed => {
                        self.tls_closed = true;
                        self.close_socket(io);
                    }
                }
            } else if let Some(down) = self.cipher_down.pop() {
                match self.socket {
                    Some(socket) => io.push(IoRequest::Stream { stream: socket, down }),
                    None => {}
                }
            } else if self.llm.has_work() && self.calling() {
                llm::resume(&mut self.llm, &llm_env, &mut self.llm_events, &mut self.plain_down);
            } else {
                break;
            }
        }
        if self.phase == Phase::Head && self.llm.response_received() {
            self.phase = Phase::Streaming;
        }
        let activity = self.llm.activity();
        if activity != self.activity {
            self.activity = activity;
            self.deadlines.activity(env.now);
        }
        self.sync_deadlines(env);
    }
}
