//! Endpoint configuration and admission (llm-connection.md, sections 3, 4
//! and 7). An admitted client owns its credential and is handed to the pool
//! when the connection machine starts it. No network traffic occurs here.

#![expect(
    clippy::single_match,
    clippy::collapsible_match,
    clippy::manual_let_else,
    reason = "explicit routing cases and checked slab insertion"
)]

use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::{Env, Id, List, Queue, Slab, Token};
use skein_llm::{self as llm, Call, Credential, Prompt};
use skein_tls::client as tls;

use crate::boundary::{Event, Refusal, Request};
use crate::call::{Connection, Phase};
use crate::endpoint::{Endpoint, EndpointError};
use crate::limits::{Limits, worst_case};

/// Configured endpoints and bounds, with no live calls or retry policy.
#[expect(missing_debug_implementations, reason = "live calls retain bearer credentials")]
pub struct Component {
    endpoints: List<Endpoint>,
    limits: Limits,
    connections: Slab<Connection>,
    slots: List<Option<Id<Connection>>>,
    cursor: u32,
}

/// The most owner events and io requests from one component entrance.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    /// The most owner events from one entrance.
    pub above: u32,
    /// The most io requests from one entrance.
    pub below: u32,
}

/// Reserve this much room before each component entrance.
pub const MAX_OUT: MaxOut = MaxOut { above: 64, below: 64 };

impl Component {
    /// Validates bounds and stream capacities before the loop starts.
    pub fn new(endpoints: List<Endpoint>, limits: &Limits) -> Result<Component, EndpointError> {
        if endpoints.len() > limits.endpoints {
            return Err(EndpointError::TooMany);
        }
        if worst_case(limits).is_none() || limits.per_endpoint > limits.connections {
            return Err(EndpointError::Limits);
        }
        if llm::client::largest_read(&limits.llm) > limits.tls.read
            || llm::client::largest_room(&limits.llm) > limits.tls.send
            || skein_tls::client::LARGEST_READ > limits.io.largest_read()
            || skein_tls::client::largest_room(&limits.tls) > limits.io.largest_room()
        {
            return Err(EndpointError::Stream);
        }
        Ok(Component {
            endpoints,
            limits: *limits,
            connections: Slab::with_capacity(limits.connections),
            slots: List::with_capacity(limits.connections),
            cursor: 0,
        })
    }

    /// Admit one call before touching the stream. `occupied` is the pool's
    /// current connection count, including connections still settling.
    /// The connection machine takes the returned prepared client.
    pub fn admit(
        &self,
        occupied: u32,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
    ) -> Result<llm::client::Client, Refusal> {
        let Some(destination) = self.endpoints.get(endpoint) else {
            return Err(Refusal::Endpoint);
        };
        if occupied >= self.limits.connections {
            return Err(Refusal::Pool);
        }
        let input = Call { owner: call, prompt, credential, endpoint: destination.llm.clone() };
        match llm::client::Client::prepare(input, &self.limits.llm) {
            Ok(client) => Ok(client),
            Err(error) => Err(Refusal::Client(error)),
        }
    }

    /// Takes an owner's call or demand. Reserve [`MAX_OUT`] first.
    pub fn down(&mut self, env: &Env<Limits>, request: Request, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        match request {
            Request::Start { call, endpoint, prompt, credential, deadlines: _ } => {
                self.start(env, call, endpoint, prompt, credential, up, io);
            }
            Request::Next { call } => {
                for slot in &self.slots {
                    match slot {
                        Some(id) => match self.connections.get_mut(*id) {
                            Some(connection) if connection.call == Some(call) => {
                                connection.next(env, id.token(), up, io);
                                break;
                            }
                            Some(_) | None => {}
                        },
                        None => {}
                    }
                }
            }
            Request::Cancel { call } => {
                for slot in &self.slots {
                    match slot {
                        Some(id) => match self.connections.get_mut(*id) {
                            Some(connection) if connection.call == Some(call) => {
                                connection.cancel(env, id.token(), up, io);
                                break;
                            }
                            Some(_) | None => {}
                        },
                        None => {}
                    }
                }
            }
        }
    }

    /// Routes an io answer by the component token echoed as its owner.
    pub fn up(&mut self, env: &Env<Limits>, event: IoEvent, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        let owner = match &event {
            IoEvent::Connecting { owner, .. }
            | IoEvent::Connected { owner }
            | IoEvent::Stream { owner, .. }
            | IoEvent::Failed { owner, .. }
            | IoEvent::Closed { owner }
            | IoEvent::Listening { owner, .. }
            | IoEvent::Accepted { owner, .. }
            | IoEvent::Output { owner, .. }
            | IoEvent::Spawned { owner, .. }
            | IoEvent::Exited { owner, .. } => *owner,
        };
        let id = Id::<Connection>::from_token(owner);
        match event {
            IoEvent::Connecting { socket, .. } => match self.connections.get_mut(id) {
                Some(connection) => {
                    connection.socket = Some(socket);
                    if connection.phase == Phase::Closing {
                        connection.close_socket(io);
                    }
                }
                None => {}
            },
            IoEvent::Connected { .. } => match self.connections.get_mut(id) {
                Some(connection) => {
                    if connection.phase == Phase::Connecting {
                        connection.start_tls(env, owner, up, io);
                    } else {
                        connection.close_socket(io);
                    }
                }
                None => {}
            },
            IoEvent::Stream { up: stream, .. } => match self.connections.get_mut(id) {
                Some(connection) => connection.cipher_up(env, owner, stream, up, io),
                None => {}
            },
            IoEvent::Failed { .. } => match self.connections.get_mut(id) {
                Some(connection) => connection.failed(env, owner, up, io),
                None => {}
            },
            IoEvent::Closed { .. } => {
                match self.connections.get_mut(id) {
                    Some(connection) => connection.closed(env, owner, up, io),
                    None => return,
                }
                for index in 0..self.slots.len() {
                    let slot = self.slots.get_mut(index).expect("the index is within the slot table");
                    match slot {
                        Some(candidate) if *candidate == id => *slot = None,
                        Some(_) | None => {}
                    }
                }
                self.connections.retire(id);
            }
            IoEvent::Listening { .. }
            | IoEvent::Accepted { .. }
            | IoEvent::Output { .. }
            | IoEvent::Spawned { .. }
            | IoEvent::Exited { .. } => {}
        }
    }

    /// Runs buffered child work and closes idle bindings after their keep time.
    pub fn fire(&mut self, env: &Env<Limits>, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        for _ in 0..self.slots.len() {
            let index = self.cursor;
            self.cursor = match self.cursor.checked_add(1) {
                Some(next) if next < self.slots.len() => next,
                Some(_) | None => 0,
            };
            match self.slots.get(index) {
                Some(Some(id)) => match self.connections.get_mut(*id) {
                    Some(connection) => {
                        match connection.idle_at {
                            Some(then) if env.now >= then.saturating_add(env.limits.idle_keep) => {
                                connection.close_idle(env);
                            }
                            Some(_) | None => {}
                        }
                        connection.route(env, id.token(), up, io);
                        break;
                    }
                    None => {}
                },
                Some(None) | None => {}
            }
        }
    }

    /// Buffered routing work that the owner should schedule before sleeping.
    #[must_use]
    pub fn has_work(&self) -> bool {
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get(*id) {
                    Some(connection) if connection.has_work() => return true,
                    Some(_) | None => {}
                },
                None => {}
            }
        }
        false
    }

    /// Releases settled connection slots at the owning loop's reclaim point.
    pub fn reclaim(&mut self) {
        self.connections.reclaim();
    }

    #[expect(clippy::too_many_arguments, reason = "an admitted Start and both explicit output queues")]
    fn start(
        &mut self,
        env: &Env<Limits>,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        let prepared = match self.admit(0, call, endpoint, prompt, credential) {
            Ok(client) => client,
            Err(why) => {
                up.push(Event::Refused { call, why });
                return;
            }
        };
        let mut same_endpoint: u32 = 0;
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get_mut(*id) {
                    Some(connection) => {
                        if connection.endpoint == endpoint {
                            same_endpoint = same_endpoint.saturating_add(1);
                            if connection.phase == Phase::Idle {
                                match connection.llm.next_call(prepared) {
                                    Ok(()) => {
                                        connection.call = Some(call);
                                        connection.phase = Phase::Calling;
                                        connection.idle_at = None;
                                        connection.start_call(env, id.token(), up, io);
                                        return;
                                    }
                                    Err(client) => {
                                        up.push(Event::Refused { call, why: Refusal::Client(llm::Error::Invalid) });
                                        drop(client);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    None => {}
                },
                None => {}
            }
        }
        if self.connections.is_full() || same_endpoint >= self.limits.per_endpoint {
            up.push(Event::Refused { call, why: Refusal::Pool });
            return;
        }
        let destination = self.endpoints.get(endpoint).expect("the endpoint was checked by admission");
        let tls = tls::Client::new(&destination.trust, destination.server_name.clone(), &self.limits.tls);
        let connection = Connection::new(endpoint, call, tls, prepared);
        let id = match self.connections.insert(connection) {
            Ok(id) => id,
            Err(_) => unreachable!("the pool was checked"),
        };
        let mut stored = false;
        for index in 0..self.slots.len() {
            let slot = self.slots.get_mut(index).expect("the index is within the slot table");
            if slot.is_none() {
                *slot = Some(id);
                stored = true;
                break;
            }
        }
        if !stored {
            self.slots.push(Some(id)).expect("a slot exists for every connection");
        }
        io.push(IoRequest::Connect { owner: id.token(), addr: destination.address });
    }
}
