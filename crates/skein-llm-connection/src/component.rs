//! Endpoint configuration and admission (llm-connection.md, sections 3, 4
//! and 7). The component keeps configured endpoints, physical bindings and
//! an owner lifecycle and admitted calls in arrival order. It knows no retry
//! or termination policy. `down` admits
//! calls and the owner close; `up` routes io settlement; `fire` advances one
//! binding. Closed is last, after every physical binding has settled.

#![expect(clippy::single_match, clippy::manual_let_else, reason = "explicit routing cases and checked slab insertion")]

use alloc::boxed::Box;

use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::{Env, Id, List, Queue, Slab, Token};
use skein_llm::{self as llm, Call, Credential, Prompt};
use skein_tls::client as tls;

use crate::boundary::{Event, Refusal, Request};
use crate::call::{Connection, Phase};
use crate::endpoint::{Endpoint, EndpointError, Transport};
use crate::limits::{Limits, worst_case};

/// The owner's call pool, ending with Closed after physical socket settlement.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lifecycle {
    Live,
    Closing,
    Aborting,
    Closed,
}

/// An admitted conversation waiting for an endpoint binding; terminal releases it.
pub(crate) struct Waiting {
    call: Token,
    endpoint: u32,
    order: u64,
    prompt: Prompt,
    credential: Credential,
    drop_reasoning: bool,
    measured: llm::client::Measured,
    reservation: u64,
    stage: Stage,
    deadlines: crate::deadlines::Table,
    asked: bool,
}

/// A granted start prepared for an available binding; terminal releases it.
struct Prepared {
    client: llm::client::Client,
    call: Token,
    reservation: u64,
    deadlines: crate::deadlines::Table,
    asked: bool,
}

/// Memory grants are global arrival order; connections are per endpoint.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// Holds its owner's start and no pool reservation.
    Memory,
    /// Holds its reservation while awaiting an endpoint's connection.
    Connection,
}

/// The owner's bounded call pool; Close or Abort ends it with one Closed.
#[expect(missing_debug_implementations, reason = "live calls retain bearer credentials")]
pub struct Component {
    endpoints: List<Endpoint>,
    limits: Limits,
    connections: Slab<Connection>,
    slots: List<Option<Id<Connection>>>,
    cursor: u32,
    lifecycle: Lifecycle,
    waiting: List<Option<Waiting>>,
    arrival: u64,
    reserved: u64,
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
    pub fn new(mut endpoints: List<Endpoint>, limits: &Limits) -> Result<Component, EndpointError> {
        if endpoints.len() > limits.endpoints {
            return Err(EndpointError::TooMany);
        }
        if limits.endpoints == 0
            || limits.connections == 0
            || limits.calls == 0
            || limits.per_endpoint == 0
            || limits.per_endpoint > limits.connections
        {
            return Err(EndpointError::Limits);
        }
        if limits.connections < limits.calls {
            return Err(EndpointError::ConnectionsCalls { connections: limits.connections, calls: limits.calls });
        }
        let mut waiting = List::with_capacity(limits.calls);
        for _ in 0..limits.calls {
            match waiting.push(None) {
                Ok(()) => {}
                Err(_) => unreachable!("one record per conversation"),
            }
        }
        for index in 0..endpoints.len() {
            let destination = endpoints.get_mut(index).expect("configured endpoint");
            destination.limits.http.request =
                match llm::client::request_head(&destination.llm, &destination.credential, &destination.limits) {
                    Ok(head) => head,
                    Err(error) => return Err(EndpointError::Client(error)),
                };
            let largest = llm::client::largest_reservation(&destination.limits).ok_or(EndpointError::Limits)?;
            if largest > limits.memory {
                return Err(EndpointError::MemoryLargestCall { memory: limits.memory, call: largest });
            }
            let read = llm::client::largest_read(&destination.limits);
            let send = llm::client::largest_room(&destination.limits);
            let sse = destination.limits.sse.chunk;
            if sse > destination.limits.http.read {
                return Err(EndpointError::SseChunkHttpRead { demand: sse, cap: destination.limits.http.read });
            }
            let tokenizer = llm::client::largest_event_demand(&destination.limits);
            if tokenizer > sse {
                return Err(EndpointError::TokenizerDemandSseChunk { demand: tokenizer, cap: sse });
            }
            match &destination.transport {
                Transport::Tls { .. } => {
                    if read > limits.tls.read {
                        return Err(EndpointError::HttpReadTlsRead { demand: read, cap: limits.tls.read });
                    }
                    if send > limits.tls.send {
                        return Err(EndpointError::HttpSendTlsSend { demand: send, cap: limits.tls.send });
                    }
                    if tls::LARGEST_READ > limits.io.largest_read() {
                        return Err(EndpointError::TlsReadIoIntake {
                            demand: tls::LARGEST_READ,
                            cap: limits.io.largest_read(),
                        });
                    }
                    if tls::largest_room(&limits.tls) > limits.io.largest_room() {
                        return Err(EndpointError::TlsSendIoOutput {
                            demand: tls::largest_room(&limits.tls),
                            cap: limits.io.largest_room(),
                        });
                    }
                }
                Transport::Plaintext => {
                    if !destination.address.ip().is_loopback() {
                        return Err(EndpointError::PlaintextAddress);
                    }
                    if read > limits.io.largest_read() {
                        return Err(EndpointError::HttpReadIoIntake { demand: read, cap: limits.io.largest_read() });
                    }
                    if send > limits.io.largest_room() {
                        return Err(EndpointError::HttpSendIoOutput { demand: send, cap: limits.io.largest_room() });
                    }
                }
            }
        }
        if worst_case(limits, &endpoints).is_none() {
            return Err(EndpointError::Limits);
        }
        Ok(Component {
            endpoints,
            limits: *limits,
            connections: Slab::with_capacity(limits.connections),
            slots: List::with_capacity(limits.connections),
            cursor: 0,
            lifecycle: Lifecycle::Live,
            waiting,
            arrival: 0,
            reserved: 0,
        })
    }

    /// Check and prepare a standalone call; down measures starts and prepares on running.
    pub fn admit(
        &self,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
    ) -> Result<llm::client::Client, Refusal> {
        self.admit_policy(call, endpoint, prompt, credential, None)
    }

    /// Checks one call with its owner's explicit model policy before any IO.
    pub fn admit_with_reasoning_drop(
        &self,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
        enabled: bool,
    ) -> Result<llm::client::Client, Refusal> {
        self.admit_policy(call, endpoint, prompt, credential, Some(enabled))
    }

    fn admit_policy(
        &self,
        call: Token,
        endpoint: u32,
        prompt: Prompt,
        credential: Credential,
        drop_reasoning: Option<bool>,
    ) -> Result<llm::client::Client, Refusal> {
        self.measure_start(endpoint, &prompt, &credential)?;
        let destination = self.endpoints.get(endpoint).expect("measured endpoint");
        let input = Call { owner: call, prompt, credential, endpoint: destination.llm.clone() };
        let enabled = match drop_reasoning {
            Some(enabled) => enabled,
            None => destination.limits.drop_reasoning,
        };
        match llm::client::Client::prepare_with_reasoning_drop(input, &destination.limits, enabled) {
            Ok(client) => Ok(client),
            Err(error) => Err(Refusal::Client(error)),
        }
    }

    fn measure_start(
        &self,
        endpoint: u32,
        prompt: &Prompt,
        credential: &Credential,
    ) -> Result<llm::client::Measured, Refusal> {
        match self.lifecycle {
            Lifecycle::Live => {}
            Lifecycle::Closing | Lifecycle::Aborting => return Err(Refusal::Closed),
            Lifecycle::Closed => unreachable!("an owner must not request work after Closed"),
        }
        let destination = self.endpoints.get(endpoint).ok_or(Refusal::Endpoint)?;
        if self.outstanding() >= self.limits.calls {
            return Err(Refusal::Calls { bound: self.limits.calls });
        }
        match llm::client::check_credential(credential, &destination.credential) {
            Ok(()) => {}
            Err(error) => return Err(Refusal::Client(error)),
        }
        let measured = match llm::client::measure(prompt, credential, &destination.llm, &destination.limits) {
            Ok(measured) => measured,
            Err(error) => return Err(Refusal::Client(error)),
        };
        let reservation =
            llm::client::reservation(measured, &destination.limits).expect("admitted largest reservation");
        if reservation > self.limits.memory {
            return Err(Refusal::Memory { reservation, bound: self.limits.memory });
        }
        Ok(measured)
    }

    /// Bytes currently granted to waiting and running calls.
    #[must_use]
    pub const fn reserved(&self) -> u64 {
        self.reserved
    }

    /// Release each running terminal once, then grant waiting memory in arrival order.
    fn refresh_memory(&mut self) {
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get_mut(*id) {
                    Some(connection) if connection.call.is_none() => {
                        self.reserved =
                            self.reserved.checked_sub(connection.reservation).expect("running reservation held");
                        connection.reservation = 0;
                    }
                    Some(_) | None => {}
                },
                None => {}
            }
        }
        match self.lifecycle {
            Lifecycle::Aborting | Lifecycle::Closed => {
                self.refresh_waiting();
                return;
            }
            Lifecycle::Live | Lifecycle::Closing => {}
        }
        for _ in 0..self.waiting.len() {
            let mut oldest = u64::MAX;
            let mut selected = None;
            for index in 0..self.waiting.len() {
                match self.waiting.get(index).expect("waiting index") {
                    Some(waiting) => match waiting.stage {
                        Stage::Memory => {
                            if waiting.order < oldest {
                                oldest = waiting.order;
                                selected = Some(index);
                            }
                        }
                        Stage::Connection => {}
                    },
                    None => {}
                }
            }
            let Some(index) = selected else {
                break;
            };
            let waiting = self.waiting.get_mut(index).expect("waiting index").as_mut().expect("oldest memory wait");
            let remaining = self.limits.memory.checked_sub(self.reserved).expect("grants fit the pool");
            if waiting.reservation > remaining {
                break;
            }
            self.reserved = self.reserved.checked_add(waiting.reservation).expect("grant fits memory");
            waiting.stage = Stage::Connection;
        }
        self.refresh_waiting();
    }

    fn release_waiting(&mut self, waiting: &Waiting) {
        match waiting.stage {
            Stage::Memory => {}
            Stage::Connection => {
                self.reserved = self.reserved.checked_sub(waiting.reservation).expect("waiting grant held");
            }
        }
    }

    fn prepare_waiting(&self, waiting: Waiting) -> Prepared {
        let destination = self.endpoints.get(waiting.endpoint).expect("admitted endpoint");
        let input = Call {
            owner: waiting.call,
            prompt: waiting.prompt,
            credential: waiting.credential,
            endpoint: destination.llm.clone(),
        };
        let prepared =
            llm::client::Client::prepare_with_reasoning_drop(input, &destination.limits, waiting.drop_reasoning)
                .expect("measurement made every preparation refusal");
        assert_eq!(prepared.measured(), waiting.measured, "admitted lengths encode identically");
        Prepared {
            client: prepared,
            call: waiting.call,
            reservation: waiting.reservation,
            deadlines: waiting.deadlines,
            asked: waiting.asked,
        }
    }

    /// Takes an owner's call or demand. Reserve [`MAX_OUT`] first.
    pub fn down(&mut self, env: &Env<Limits>, request: Request, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        self.down_event(env, request, up, io);
        self.refresh_memory();
    }

    fn down_event(&mut self, env: &Env<Limits>, request: Request, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        assert!(self.lifecycle != Lifecycle::Closed, "an owner must not send requests after Closed");
        match request {
            Request::Close => {
                match self.lifecycle {
                    Lifecycle::Live => self.lifecycle = Lifecycle::Closing,
                    Lifecycle::Closing | Lifecycle::Aborting => return,
                    Lifecycle::Closed => unreachable!("checked at the entrance"),
                }
                self.request_close(false);
                self.fire(env, up, io);
            }
            Request::Abort => {
                match self.lifecycle {
                    Lifecycle::Live | Lifecycle::Closing => {}
                    Lifecycle::Aborting => return,
                    Lifecycle::Closed => unreachable!("checked at the entrance"),
                }
                self.lifecycle = Lifecycle::Aborting;
                self.request_close(true);
                self.fire(env, up, io);
            }
            Request::Start { call, endpoint, prompt, credential, deadlines, drop_reasoning } => {
                self.start(env, call, endpoint, prompt, credential, deadlines, drop_reasoning, up, io);
            }
            Request::Next { call } => {
                for index in 0..self.waiting.len() {
                    match self.waiting.get_mut(index).expect("waiting index") {
                        Some(waiting) if waiting.call == call => {
                            waiting.asked = true;
                            return;
                        }
                        Some(_) | None => {}
                    }
                }
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
                for index in 0..self.waiting.len() {
                    match self.waiting.get(index).expect("waiting index") {
                        Some(waiting) if waiting.call == call => {
                            let waiting = self
                                .waiting
                                .get_mut(index)
                                .expect("waiting index")
                                .take()
                                .expect("cancelled waiting call");
                            self.release_waiting(&waiting);
                            up.push(Event::Cancelled { call });
                            self.refresh_waiting();
                            return;
                        }
                        Some(_) | None => {}
                    }
                }
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
        self.up_event(env, event, up, io);
        self.refresh_memory();
    }

    fn up_event(&mut self, env: &Env<Limits>, event: IoEvent, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
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
            IoEvent::Usage { .. } | IoEvent::Shutdown { .. } => return,
        };
        let id = Id::<Connection>::from_token(owner);
        match event {
            IoEvent::Connecting { socket, .. } => match self.connections.get_mut(id) {
                Some(connection) => {
                    connection.socket = Some(socket);
                    if connection.phase == Phase::Closing {
                        connection.close_socket(io);
                    }
                    connection.route(env, owner, up, io);
                }
                None => {}
            },
            IoEvent::Connected { .. } => match self.connections.get_mut(id) {
                Some(connection) => {
                    if connection.phase == Phase::Connecting {
                        connection.connected(env, owner, up, io);
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
                self.refresh_memory();
                for index in 0..self.slots.len() {
                    let slot = self.slots.get_mut(index).expect("the index is within the slot table");
                    match slot {
                        Some(candidate) if *candidate == id => *slot = None,
                        Some(_) | None => {}
                    }
                }
                self.connections.retire(id);
                self.settled(up);
            }
            IoEvent::Listening { .. }
            | IoEvent::Accepted { .. }
            | IoEvent::Output { .. }
            | IoEvent::Spawned { .. }
            | IoEvent::Exited { .. }
            | IoEvent::Usage { .. }
            | IoEvent::Shutdown { .. } => {}
        }
    }

    /// Runs buffered child work and closes idle bindings after their keep time.
    pub fn fire(&mut self, env: &Env<Limits>, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        self.refresh_memory();
        self.fire_event(env, up, io);
        self.refresh_memory();
    }

    fn fire_event(&mut self, env: &Env<Limits>, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) {
        for index in 0..self.waiting.len() {
            match self.waiting.get(index).expect("waiting index") {
                Some(waiting) if self.lifecycle == Lifecycle::Aborting || waiting.deadlines.due(env.now).is_some() => {
                    let waiting = self.waiting.get_mut(index).expect("waiting index").take().expect("waiting call");
                    self.release_waiting(&waiting);
                    if self.lifecycle == Lifecycle::Aborting {
                        up.push(Event::Cancelled { call: waiting.call });
                    } else {
                        let bound =
                            self.endpoints.get(waiting.endpoint).expect("admitted endpoint").limits.detail_bytes;
                        let detail = b"timed out waiting for the whole call";
                        let take = detail.len().min(usize::try_from(bound).expect("u32 fits usize"));
                        up.push(Event::Failed {
                            call: waiting.call,
                            failure: llm::Failure::TimedOut { phase: llm::Phase::Whole },
                            evidence: llm::client::Evidence::Unsent,
                            detail: Box::from(detail.get(..take).expect("bounded detail")),
                        });
                    }
                    self.refresh_waiting();
                    self.settled(up);
                    return;
                }
                Some(_) | None => {}
            }
        }
        if self.serve_waiting(env, up, io) {
            return;
        }
        for _ in 0..self.slots.len() {
            let index = self.cursor;
            self.cursor = match self.cursor.checked_add(1) {
                Some(next) if next < self.slots.len() => next,
                Some(_) | None => 0,
            };
            match self.slots.get(index) {
                Some(Some(id)) => match self.connections.get_mut(*id) {
                    Some(connection) => {
                        if !connection.has_work() && connection.deadlines.due(env.now).is_none() {
                            continue;
                        }
                        if !connection.aborting() {
                            match connection.deadlines.due(env.now) {
                                Some(due) => connection.timeout(env, due, io),
                                None => {}
                            }
                        }
                        connection.route(env, id.token(), up, io);
                        break;
                    }
                    None => {}
                },
                Some(None) | None => {}
            }
        }
        self.settled(up);
    }

    /// Buffered routing work that the owner should schedule before sleeping.
    #[must_use]
    pub fn has_work(&self) -> bool {
        if self.runnable_waiting().is_some() || (self.lifecycle == Lifecycle::Aborting && self.waiting_count() > 0) {
            return true;
        }
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

    /// The first call or idle-connection deadline for the owning loop.
    #[must_use]
    pub fn next_deadline(&self) -> Option<skein_lib::Time> {
        let mut earliest: Option<skein_lib::Time> = None;
        for waiting in &self.waiting {
            match waiting {
                Some(waiting) => match waiting.deadlines.next() {
                    Some(next) => {
                        earliest = Some(match earliest {
                            Some(prior) => prior.min(next),
                            None => next,
                        });
                    }
                    None => {}
                },
                None => {}
            }
        }
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get(*id) {
                    Some(connection) => {
                        let next = connection.deadlines.next();
                        earliest = match earliest {
                            Some(prior) => match next {
                                Some(next) => Some(prior.min(next)),
                                None => Some(prior),
                            },
                            None => next,
                        };
                    }
                    None => {}
                },
                None => {}
            }
        }
        earliest
    }

    fn request_close(&mut self, abort: bool) {
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get_mut(*id) {
                    Some(connection) => {
                        connection.request_close(abort);
                    }
                    None => {}
                },
                None => {}
            }
        }
    }

    fn settled(&mut self, up: &mut Queue<Event>) {
        match self.lifecycle {
            Lifecycle::Live | Lifecycle::Closed => return,
            Lifecycle::Closing | Lifecycle::Aborting => {}
        }
        if self.waiting_count() > 0 {
            return;
        }
        for slot in &self.slots {
            if slot.is_some() {
                return;
            }
        }
        self.lifecycle = Lifecycle::Closed;
        up.push(Event::Closed);
    }

    /// Checked heap bound using the normalized limits of configured endpoints.
    #[must_use]
    pub fn worst_case(&self) -> Option<u64> {
        worst_case(&self.limits, &self.endpoints)
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
        deadlines: crate::Deadlines,
        drop_reasoning: bool,
        up: &mut Queue<Event>,
        io: &mut Queue<IoRequest>,
    ) {
        let measured = match self.measure_start(endpoint, &prompt, &credential) {
            Ok(measured) => measured,
            Err(why) => {
                up.push(Event::Refused { call, why });
                return;
            }
        };
        let destination = self.endpoints.get(endpoint).expect("admitted endpoint");
        let reservation = llm::client::reservation(measured, &destination.limits).expect("measured largest call fits");
        let mut table = crate::deadlines::Table::new(deadlines, env.now);
        table.arm(crate::deadlines::Phase::Waiting, env.now, self.limits.idle_keep);
        let waiting = Waiting {
            call,
            endpoint,
            order: self.arrival,
            prompt,
            credential,
            drop_reasoning,
            measured,
            reservation,
            stage: Stage::Memory,
            deadlines: table,
            asked: false,
        };
        self.arrival = self.arrival.checked_add(1).expect("arrival counter fits the process lifetime");
        for index in 0..self.waiting.len() {
            if self.waiting.get(index).expect("waiting index").is_none() {
                *self.waiting.get_mut(index).expect("waiting index") = Some(waiting);
                self.refresh_memory();
                self.serve_waiting(env, up, io);
                return;
            }
        }
        unreachable!("admission reserves a waiting record");
    }

    fn waiting_count(&self) -> u32 {
        let mut count = 0_u32;
        for waiting in &self.waiting {
            if waiting.is_some() {
                count = count.checked_add(1).expect("bounded conversations");
            }
        }
        count
    }

    fn outstanding(&self) -> u32 {
        let mut count = self.waiting_count();
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get(*id) {
                    Some(connection) if connection.call.is_some() => {
                        count = count.checked_add(1).expect("bounded conversations");
                    }
                    Some(_) | None => {}
                },
                None => {}
            }
        }
        count
    }

    fn refresh_waiting(&mut self) {
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get_mut(*id) {
                    Some(connection) => {
                        connection.waiting = false;
                        for waiting in &self.waiting {
                            match waiting {
                                Some(waiting) if waiting.endpoint == connection.endpoint => match waiting.stage {
                                    Stage::Memory => {}
                                    Stage::Connection => connection.waiting = true,
                                },
                                Some(_) | None => {}
                            }
                        }
                    }
                    None => {}
                },
                None => {}
            }
        }
    }

    // Arrival order is independent of which record was vacated by cancellation.
    fn runnable_waiting(&self) -> Option<u32> {
        if self.lifecycle == Lifecycle::Aborting || self.lifecycle == Lifecycle::Closed {
            return None;
        }
        let mut selected: Option<u32> = None;
        let mut oldest = u64::MAX;
        for index in 0..self.waiting.len() {
            match self.waiting.get(index).expect("waiting index") {
                Some(waiting) => {
                    match waiting.stage {
                        Stage::Memory => continue,
                        Stage::Connection => {}
                    }

                    let mut count = 0_u32;
                    let mut reusable = false;
                    let mut evictable = false;
                    for slot in &self.slots {
                        match slot {
                            Some(id) => match self.connections.get(*id) {
                                Some(connection) => {
                                    if connection.endpoint == waiting.endpoint {
                                        count = count.checked_add(1).expect("bounded connections");
                                        if connection.phase == Phase::Idle || connection.phase == Phase::Ready {
                                            reusable = true;
                                        }
                                    } else if connection.phase == Phase::Idle {
                                        evictable = true;
                                    }
                                }
                                None => {}
                            },
                            None => {}
                        }
                    }
                    if waiting.order < oldest
                        && (reusable
                            || (count < self.limits.per_endpoint && (!self.connections.is_full() || evictable)))
                    {
                        selected = Some(index);
                        oldest = waiting.order;
                    }
                }
                None => {}
            }
        }
        selected
    }

    fn reusable_connection(&self, endpoint: u32) -> Option<Id<Connection>> {
        for slot in &self.slots {
            match slot {
                Some(id) => match self.connections.get(*id) {
                    Some(connection)
                        if connection.endpoint == endpoint
                            && (connection.phase == Phase::Idle || connection.phase == Phase::Ready) =>
                    {
                        return Some(*id);
                    }
                    Some(_) | None => {}
                },
                None => {}
            }
        }
        None
    }

    fn serve_waiting(&mut self, env: &Env<Limits>, up: &mut Queue<Event>, io: &mut Queue<IoRequest>) -> bool {
        let index = match self.runnable_waiting() {
            Some(index) => index,
            None => return false,
        };
        let waiting = self.waiting.get(index).expect("waiting index").as_ref().expect("waiting call");
        if waiting.deadlines.due(env.now).is_some() {
            return false; // fire answers the expired wait before preparing it.
        }
        let endpoint = waiting.endpoint;
        let reusable = self.reusable_connection(endpoint);
        let prepared = if reusable.is_some() || !self.connections.is_full() {
            let waiting = self.waiting.get_mut(index).expect("waiting index").take().expect("waiting call");
            Some(self.prepare_waiting(waiting))
        } else {
            None
        };
        let mut prepared = prepared;
        if let Some(id) = reusable {
            let waiting = prepared.take().expect("reusable client prepared");
            let connection = self.connections.get_mut(id).expect("selected reusable binding");
            match connection.llm.next_call(waiting.client) {
                Ok(()) => {}
                Err(_) => unreachable!("a reusable client accepts the prepared call"),
            }
            connection.reservation = waiting.reservation;
            connection.call = Some(waiting.call);
            connection.phase = Phase::Head;
            connection.idle_at = None;
            connection.deadlines = waiting.deadlines;
            connection.sync_deadlines(env);
            if waiting.asked {
                connection.next(env, id.token(), up, io);
            }
            connection.start_call(env, id.token(), up, io);
            self.refresh_waiting();
            return true;
        }
        if self.connections.is_full() {
            for slot in &self.slots {
                match slot {
                    Some(id) => match self.connections.get_mut(*id) {
                        Some(connection) if connection.endpoint != endpoint && connection.phase == Phase::Idle => {
                            connection.request_close(false);
                            connection.route(env, id.token(), up, io);
                            return true;
                        }
                        Some(_) | None => {}
                    },
                    None => {}
                }
            }
            return false;
        }
        let waiting = prepared.take().expect("new client prepared");
        let destination = self.endpoints.get(endpoint).expect("admitted endpoint");
        let tls = match &destination.transport {
            Transport::Tls { server_name, trust } => {
                Some(tls::Client::new(trust, server_name.clone(), &self.limits.tls))
            }
            Transport::Plaintext => None,
        };
        let mut connection = Connection::new(
            endpoint,
            waiting.call,
            tls,
            waiting.client,
            destination.limits,
            crate::Deadlines::none(),
            env.now,
        );
        connection.reservation = waiting.reservation;
        connection.deadlines = waiting.deadlines;
        connection.sync_deadlines(env);
        if self.lifecycle == Lifecycle::Closing {
            connection.request_close(false);
        }
        let id = match self.connections.insert(connection) {
            Ok(id) => id,
            Err(_) => unreachable!("checked connection capacity"),
        };
        let mut stored = false;
        for index in 0..self.slots.len() {
            let slot = self.slots.get_mut(index).expect("connection index");
            if slot.is_none() {
                *slot = Some(id);
                stored = true;
                break;
            }
        }
        if !stored {
            self.slots.push(Some(id)).expect("one slot per connection");
        }
        io.push(IoRequest::Connect { owner: id.token(), addr: destination.address });
        if waiting.asked {
            self.connections.get_mut(id).expect("new connection").next(env, id.token(), up, io);
        }
        self.refresh_waiting();
        true
    }
}

#[cfg(test)]
mod memory_tests {
    use super::*;
    use crate::tests::{component, credential, endpoint, limits, prompt};

    #[test]
    fn an_entrance_larger_than_the_pool_is_refused_before_preparing() {
        // Construction normally excludes this case; lower the private pool to
        // exercise the entrance guard independently of startup validation.
        let mut component = component();
        let destination = component.endpoints.get(0).unwrap();
        let input = prompt();
        let secret = credential();
        let measured = skein_llm::client::measure(&input, &secret, &destination.llm, &destination.limits).unwrap();
        let reservation = skein_llm::client::reservation(measured, &destination.limits).unwrap();
        component.limits.memory = reservation - 1;
        assert_eq!(
            component.admit(Token::new(7), 0, input, secret).err(),
            Some(Refusal::Memory { reservation, bound: reservation - 1 })
        );
        assert_eq!(component.reserved(), 0);
    }

    #[test]
    fn a_younger_fitting_start_stays_behind_an_older_memory_wait() {
        let destination = endpoint();
        let mut bounds = destination.limits;
        bounds.http.request = llm::client::request_head(&destination.llm, &destination.credential, &bounds).unwrap();
        let measured = llm::client::measure(&prompt(), &credential(), &destination.llm, &bounds).unwrap();
        let reservation = llm::client::reservation(measured, &bounds).unwrap();
        let mut config = limits();
        config.calls = 3;
        config.connections = 3;
        config.per_endpoint = 3;
        config.io.sockets = 3;
        config.memory = 2 * reservation;
        let mut endpoints = List::with_capacity(1);
        endpoints.push(destination).unwrap();
        let mut pool = Component::new(endpoints, &config).unwrap();
        let env = Env { now: skein_lib::Time::ZERO, wall: skein_lib::Wall::EPOCH, limits: config };
        let mut up = Queue::with_capacity(MAX_OUT.above);
        let mut io = Queue::with_capacity(MAX_OUT.below);
        for token in [7, 8, 9] {
            let mut input = prompt();
            if token == 8 {
                input.instructions = Box::new([b'x'; 1024]);
            }
            pool.down(
                &env,
                Request::Start {
                    call: Token::new(token),
                    endpoint: 0,
                    prompt: input,
                    credential: credential(),
                    deadlines: crate::Deadlines::none(),
                    drop_reasoning: false,
                },
                &mut up,
                &mut io,
            );
        }
        assert_eq!(pool.reserved(), reservation);
        assert_eq!(io.len(), 1, "only the first call prepares and connects");
        assert_eq!(pool.waiting_count(), 2);
        pool.down(&env, Request::Cancel { call: Token::new(8) }, &mut up, &mut io);
        assert_eq!(pool.reserved(), 2 * reservation, "removing the older wait grants the younger call");
        assert_eq!(pool.waiting_count(), 1);
    }

    #[test]
    fn increasing_pool_memory_adds_exactly_that_many_bytes_to_the_bound() {
        let mut pool = component();
        let before = pool.worst_case().unwrap();
        pool.limits.memory += 1024;
        assert_eq!(pool.worst_case().unwrap() - before, 1024);
    }
}
