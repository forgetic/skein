//! Shared io passes and declared queue/connection bounds; no peer semantics.

use std::net::SocketAddr;

use skein_io::{self as io, kernel};
use skein_lib::{Env, Queue, Time, Token, Wall};

pub(crate) const LISTENER: Token = Token::new(u64::MAX);

/// Fixed connection, plaintext/TLS buffer and observation caps supplied by a world.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub io: io::Limits,
    pub connections: u32,
    /// Each internal queue's record count; at least 64.
    pub queue: u32,
    pub plaintext: u32,
    pub ciphertext: u32,
    pub observations: u32,
    /// Total owned payload bytes held by observations.
    pub observation_bytes: u32,
}

/// Constructor refusal returned to the world before any effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The world supplied unusable limits, configuration or a non-loopback address.
    Limits,
}

pub(crate) struct Face {
    pub io: io::Io,
    pub limits: Limits,
    pub events: Queue<io::Event>,
    pub requests: Queue<io::Request>,
    pub submissions: Queue<kernel::Submit>,
    pub completions: Queue<kernel::Complete>,
    pub listener: Option<Token>,
    pub address: Option<SocketAddr>,
    pub stopping: bool,
}

impl Face {
    pub fn new(address: SocketAddr, limits: Limits) -> Result<Self, Error> {
        if !address.ip().is_loopback() || worst_case(&limits).is_none() {
            return Err(Error::Limits);
        }
        let mut requests = Queue::with_capacity(limits.queue);
        requests.push(io::Request::Listen { owner: LISTENER, addr: address });
        Ok(Self {
            io: io::Io::new(&limits.io),
            limits,
            events: Queue::with_capacity(limits.queue),
            requests,
            submissions: Queue::with_capacity(limits.queue),
            completions: Queue::with_capacity(limits.queue),
            listener: None,
            address: None,
            stopping: false,
        })
    }

    pub fn up(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits.io };
        for _ in 0..self.limits.queue {
            if self.events.room() < 3 || self.submissions.room() < 2 {
                break;
            }
            if self.io.is_ready() {
                io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
            } else {
                break;
            }
        }
        for _ in 0..self.limits.queue {
            if self.events.room() < 3 || self.submissions.room() < 2 {
                break;
            }
            let Some(complete) = self.completions.pop() else { break };
            io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
        }
        for _ in 0..self.limits.queue {
            if self.events.room() < 3 || self.submissions.room() < 2 || !self.io.is_due(now) {
                break;
            }
            io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
    }

    pub fn down(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits.io };
        for _ in 0..self.limits.queue {
            if self.submissions.room() < 2 || !self.io.takes() {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }

    pub fn shutdown(&mut self) {
        self.stopping = true;
    }

    pub fn close_listener(&mut self) {
        if self.stopping
            && self.requests.room() > 0
            && let Some(listener) = self.listener.take()
        {
            self.requests.push(io::Request::Close { entity: listener });
        }
    }

    pub fn work_pending(&self) -> bool {
        self.io.is_ready()
            || !self.completions.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || (self.stopping && self.listener.is_some())
    }

    pub fn is_empty(&self) -> bool {
        self.io.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
    }
}

pub(crate) fn worst_case(limits: &Limits) -> Option<u64> {
    if !limits.io.is_usable()
        || limits.queue < 64
        || limits.connections == 0
        || limits.io.sockets < limits.connections.checked_add(1)?
        || limits.plaintext == 0
        || limits.ciphertext < 18_432
        || limits.io.intake < 18_432
        || limits.io.output < limits.ciphertext
    {
        return None;
    }
    io::worst_case(&limits.io)?
        .checked_add(Queue::<io::Event>::worst_case(limits.queue)?)?
        .checked_add(Queue::<io::Request>::worst_case(limits.queue)?)?
        .checked_add(Queue::<kernel::Complete>::worst_case(limits.queue)?)?
        .checked_add(Queue::<kernel::Submit>::worst_case(limits.queue)?)?
        // Returned receives and queued sends coexist at the io boundary.
        .checked_add(u64::from(limits.queue).checked_mul(u64::from(limits.io.receive.max(limits.io.output)))?)
}
