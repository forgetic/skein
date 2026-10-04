//! A prompt side below for a TLS client: a rustls server's ciphertext, each
//! demand answered as soon as it can be, room granted at once. Where the
//! world draws its neighbours from a seed, this one is plain, for tests that
//! aim at one outcome: a handshake's, a stack's.
//!
//! It checks the client's requests below as the world does: one demand at a
//! time, fills only, each `Send` within the room granted.

use std::collections::VecDeque;

use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Queue};
use skein_tls::client::{self, Client, Event, Limits, Request};

use crate::server::Server;

/// The stream below a client, to a server.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "the stream's state, a flag each")]
pub struct Wire {
    pub server: Server,
    /// The server's bytes on their way, and what else is on the wire: a
    /// test may put anything there.
    pub bytes: VecDeque<u8>,
    /// The stream ends once the bytes run out.
    pub eof: bool,
    /// The client's demand outstanding: its read, and its room.
    pub demand: Option<(Read, u32)>,
    granted: Option<u32>,
    pub ended: bool,
    pub finished: bool,
    /// The length of each `Send`, in order.
    pub sends: Vec<usize>,
    /// What the client sent, all of it.
    pub sent: Vec<u8>,
    /// Whether the server reads what the client sends: not once its keys
    /// were taken out.
    pub forward: bool,
}

impl Wire {
    #[must_use]
    pub fn new(server: Server) -> Wire {
        Wire {
            server,
            bytes: VecDeque::new(),
            eof: false,
            demand: None,
            granted: None,
            ended: false,
            finished: false,
            sends: Vec::new(),
            sent: Vec::new(),
            forward: true,
        }
    }

    /// What the server has to send, put on the wire.
    pub fn pull(&mut self) {
        let records = self.server.transmit();
        self.bytes.extend(records);
    }

    /// A request of the client's, checked, and carried out.
    pub fn take(&mut self, request: Down) {
        match request {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(self.demand.take().is_some(), "only a demand outstanding is withdrawn");
            }
            Down::Demand { read, room } => {
                assert!(self.demand.is_none(), "one demand at a time");
                match read {
                    Read::Nothing => {}
                    Read::Fill(n) => assert!(n <= client::LARGEST_READ && !self.ended, "a fill within the cap"),
                    Read::Scan { .. } | Read::Line { .. } => panic!("the client reads records by fills"),
                }
                if room > 0 {
                    assert!(!self.finished, "no room after Finish");
                    self.granted = None;
                }
                self.demand = Some((read, room));
            }
            Down::Send(bytes) => {
                let granted = self.granted.take().expect("a Send within room granted");
                assert!(bytes.len() <= usize::try_from(granted).expect("fits"), "a Send within room granted");
                self.sends.push(bytes.len());
                self.sent.extend_from_slice(&bytes);
                if self.forward {
                    self.server.receive(&bytes);
                    self.pull();
                }
            }
            Down::Finish => {
                assert!(!self.finished, "one Finish");
                self.finished = true;
            }
        }
    }

    /// The answer to the demand outstanding, if one can be given: the bytes
    /// it reads, room, or the end of the stream.
    pub fn answer(&mut self) -> Option<Up> {
        let (read, room) = self.demand?;
        if let Read::Fill(n) = read {
            let n = usize::try_from(n).expect("fits");
            if !self.ended && self.bytes.len() >= n {
                self.demand = None;
                let bytes: Vec<u8> = self.bytes.drain(..n).collect();
                return Some(Up::Bytes(bytes.into()));
            }
        }
        if room > 0 {
            self.demand = None;
            self.granted = Some(room);
            return Some(Up::Room);
        }
        if read != Read::Nothing && self.eof && !self.ended {
            self.ended = true;
            // The read stays outstanding, never met, as io keeps it.
            return Some(Up::End);
        }
        None
    }
}

/// A client and its wire, driven promptly.
#[derive(Debug)]
pub struct Pair {
    pub client: Client,
    pub env: Env<Limits>,
    pub wire: Wire,
    events: Queue<Event>,
    requests: Queue<Down>,
}

impl Pair {
    #[must_use]
    pub fn new(client: Client, env: Env<Limits>, server: Server) -> Pair {
        Pair {
            client,
            env,
            wire: Wire::new(server),
            events: Queue::with_capacity(8),
            requests: Queue::with_capacity(8),
        }
    }

    /// A request of the side above's, and what came of it.
    pub fn down(&mut self, rq: Request) -> Vec<Event> {
        client::down(&mut self.client, &self.env, rq, &mut self.events, &mut self.requests);
        self.route(client::DOWN_MAX_OUT)
    }

    /// An event from below, and what came of it.
    pub fn up(&mut self, ev: Up) -> Vec<Event> {
        client::up(&mut self.client, &self.env, ev, &mut self.events, &mut self.requests);
        self.route(client::UP_MAX_OUT)
    }

    /// Answers the client's demands below until none can be answered, and
    /// what came of them.
    pub fn settle(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        for _ in 0..100_000 {
            let Some(answer) = self.wire.answer() else { return events };
            events.extend(self.up(answer));
        }
        panic!("the wire settles");
    }

    fn route(&mut self, max: skein_tls::MaxOut) -> Vec<Event> {
        assert!(self.events.len() <= max.above, "MAX_OUT honoured above");
        assert!(self.requests.len() <= max.below, "MAX_OUT honoured below");
        while let Some(request) = self.requests.pop() {
            self.wire.take(request);
        }
        let mut events = Vec::new();
        while let Some(event) = self.events.pop() {
            events.push(event);
        }
        events
    }
}
