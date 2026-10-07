//! One physical connection and its current call (llm-connection.md, sections
//! 4 and 5). A connection keeps its socket name, TLS and LLM machines, and
//! their bounded routing queues. It never knows the owner's domain or retry
//! policy. `route` advances the classic TLS stream and the LLM client.
//!
//! | State | Input | Next | Emits |
//! | --- | --- | --- | --- |
//! | Connecting | connected | Handshaking | TLS handshake |
//! | Handshaking | ready | Calling | LLM request |
//! | Calling | drained | Idle | answer, reusable binding |
//! | any live | failure or close | Closing | one call terminal, socket close |
//! | Closing | socket closed | Closed | settlement |

#![expect(clippy::single_match, reason = "explicit socket presence cases at the stream boundary")]

use skein_io::Request as IoRequest;
use skein_lib::stream::{Down, Up};
use skein_lib::{Env, Queue, Time, Token};
use skein_llm::client as llm;
use skein_tls::client as tls;

use crate::boundary::Event;
use crate::limits::Limits;

/// The physical stream's lifecycle, independent of the call's terminal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Phase {
    Connecting,
    Handshaking,
    Calling,
    Idle,
    Closing,
}

/// One socket, TLS session and LLM client, named to io by its slab token.
pub(crate) struct Connection {
    pub(crate) endpoint: u32,
    pub(crate) call: Option<Token>,
    pub(crate) socket: Option<Token>,
    pub(crate) phase: Phase,
    pub(crate) tls: tls::Client,
    pub(crate) llm: llm::Client,
    pub(crate) idle_at: Option<Time>,
    pub(crate) tls_closed: bool,
    pub(crate) socket_close_sent: bool,
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
            || (self.phase == Phase::Calling && self.llm.has_work())
    }

    pub(crate) fn new(endpoint: u32, call: Token, tls: tls::Client, llm: llm::Client) -> Connection {
        Connection {
            endpoint,
            call: Some(call),
            socket: None,
            phase: Phase::Connecting,
            tls,
            llm,
            idle_at: None,
            tls_closed: false,
            socket_close_sent: false,
            llm_events: Queue::with_capacity(64),
            plain_down: Queue::with_capacity(256),
            tls_events: Queue::with_capacity(64),
            cipher_down: Queue::with_capacity(256),
        }
    }

    pub(crate) fn start_tls(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        self.phase = Phase::Handshaking;
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        tls::down(&mut self.tls, &tls_env, tls::Request::Handshake, &mut self.tls_events, &mut self.cipher_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn start_call(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
        llm::down(&mut self.llm, &llm_env, llm::Request::Start, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn next(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
        llm::down(&mut self.llm, &llm_env, llm::Request::Next, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn cancel(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
        llm::down(&mut self.llm, &llm_env, llm::Request::Cancel, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
        if self.phase == Phase::Connecting {
            self.close_socket(io);
        }
    }

    pub(crate) fn failed(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
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
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        tls::up(&mut self.tls, &tls_env, event, &mut self.tls_events, &mut self.cipher_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn closed(&mut self, env: &Env<Limits>, owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
        llm::closed(&mut self.llm, &llm_env, &mut self.llm_events, &mut self.plain_down);
        self.route(env, owner, up, io);
    }

    pub(crate) fn close_socket(&mut self, io: &mut Queue<IoRequest>) {
        if !self.socket_close_sent {
            match self.socket {
                Some(socket) => {
                    if self.tls_closed {
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

    pub(crate) fn close_idle(&mut self, env: &Env<Limits>) {
        if self.phase == Phase::Idle {
            let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
            tls::down(&mut self.tls, &tls_env, tls::Request::Close, &mut self.tls_events, &mut self.cipher_down);
            self.phase = Phase::Closing;
        }
    }

    pub(crate) fn route(&mut self, env: &Env<Limits>, _owner: Token, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let llm_env = Env { now: env.now, wall: env.wall, limits: env.limits.llm };
        let tls_env = Env { now: env.now, wall: env.wall, limits: env.limits.tls };
        for _ in 0_u32..64_u32 {
            if let Some(event) = self.llm_events.pop() {
                match event {
                    llm::Event::Delta { owner: call, delta } => up.push(Event::Delta { call, delta }),
                    llm::Event::Block { owner: call, block } => up.push(Event::Block { call, block }),
                    llm::Event::Completed { owner: call, completion } => {
                        up.push(Event::Completed { call, completion });
                        self.call = None;
                    }
                    llm::Event::Failed { owner: call, failure, evidence, detail } => {
                        up.push(Event::Failed { call, failure, evidence, detail });
                        self.call = None;
                    }
                    llm::Event::Cancelled { owner: call } => {
                        up.push(Event::Cancelled { call });
                        self.call = None;
                    }
                    llm::Event::Reusable => {
                        self.phase = Phase::Idle;
                        self.idle_at = Some(env.now);
                    }
                    llm::Event::Close => match self.phase {
                        Phase::Handshaking | Phase::Calling | Phase::Idle => {
                            tls::down(
                                &mut self.tls,
                                &tls_env,
                                tls::Request::Close,
                                &mut self.tls_events,
                                &mut self.cipher_down,
                            );
                            self.phase = Phase::Closing;
                        }
                        Phase::Connecting => self.close_socket(io),
                        Phase::Closing => {}
                    },
                    llm::Event::Closed => {}
                }
            } else if let Some(down) = self.plain_down.pop() {
                if self.phase == Phase::Calling || self.phase == Phase::Idle {
                    tls::down(
                        &mut self.tls,
                        &tls_env,
                        tls::Request::Stream(down),
                        &mut self.tls_events,
                        &mut self.cipher_down,
                    );
                }
            } else if let Some(event) = self.tls_events.pop() {
                match event {
                    tls::Event::Ready(_) => {
                        self.phase = Phase::Calling;
                        llm::down(
                            &mut self.llm,
                            &llm_env,
                            llm::Request::Start,
                            &mut self.llm_events,
                            &mut self.plain_down,
                        );
                    }
                    tls::Event::Stream(plain) => {
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
            } else if self.llm.has_work() && self.phase == Phase::Calling {
                llm::resume(&mut self.llm, &llm_env, &mut self.llm_events, &mut self.plain_down);
            } else {
                break;
            }
        }
    }
}
